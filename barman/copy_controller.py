# Copyright (C) 2011-2016 2ndQuadrant Italia Srl
#
# This file is part of Barman.
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.
"""
Copy controller module

A copy controller will handle the copy between a PostgreSQL data directory,
tablespaces included, and his final destination. The `CopyController` class
is a generic manager of copy procedures, and is then specialised in a
RsyncCopyController, which copies data files in a parallel way.
"""
import logging
import os.path
from abc import ABCMeta, abstractmethod
from multiprocessing import Pool

from barman.command_wrappers import Rsync, RsyncPgData
from barman.exceptions import (CommandFailedException, DataTransferFailure,
                               RsyncListFilesFailure)
from barman.utils import mkpath, with_metaclass

_logger = logging.getLogger(__name__)


def execute_job(job):
    return job.execute()


class CopyController(with_metaclass(ABCMeta, object)):
    """
    This is a generic class for parallel copy jobs. This class implements the
    multiprocess job management, while the actual job construction is
    delegated to the subclasses.
    """
    config = None

    def __init__(self):
        super(CopyController, self).__init__()
        self.workers = None
        self.jobs_list = []

    def copy(self):
        """
        Do the backup job in a parallel way, collecting task and scheduling
        with the configured concurrent processors

        :param barman.infofile.BackupInfo previous_backup: the previous
          backup, if there is one, or None
        :param barman.infofile.BackupInfo backup_info: the backup info of the
          current backup
        """

        # Store backup parameters inside the instance. These variables
        # will be used for job planning
        # self.previous_backup = previous_backup
        # self.backup_info = backup_info

        # Jobs are created and collected in the `jobs_list` instance variable
        self.start_copy()

        # Jobs are executed using a parallel process pool
        pool = Pool(processes=self.workers)
        try:
            for result in pool.imap_unordered(execute_job, self.jobs_list):
                _logger.debug(result)
        except DataTransferFailure as e:
            _logger.exception(e)
            raise

        # Post-copy actions, if needed, are executed
        self.stop_copy()

    @abstractmethod
    def start_copy(self):
        """
        This abstract method is supposed to add to the job list the jobs
        to be executed.
        """

    # noinspection PyMethodMayBeStatic
    def stop_copy(self):
        """
        This method is executed after the backup job is completed
        and can execute post-copy actions if needed
        :return:
        """


class FileBasedCopyController(CopyController):
    """
    This copy controller starts from the structural copy and can be used when
    copy jobs are file-based.
    """

    def __init__(self, safe_horizon, reuse_backup, path):
        super(FileBasedCopyController, self).__init__()
        self.path = path
        self.reuse_backup = reuse_backup
        self.safe_horizon = safe_horizon
        self.dir_info = []

    def start_copy(self):
        # The base class will collect tablespaces and creates
        # files list for every tablespace and for pgdata
        super(FileBasedCopyController, self).start_copy()

        # TODO Ordering and backpack

        # Create the actual copy tasks
        for info in self.dir_info:
            # tbs = info['tbs']

            self.create_jobs(
                info['src'],
                info['dst'],
                info['safe_items'],
                checksum=False,
                bwlimit=info['bwlimit']
            )
            self.create_jobs(
                info['src'],
                info['dst'],
                info['check_items'],
                checksum=True,
                bwlimit=info['bwlimit']
            )

    def add_directory(self, id, src_dir, dst_dir, bwlimit, ref_dir,
                      tbs=None, exclude=None):
        dir_list = []
        safe_items = []
        check_items = []

        # Ensure that we always work on a list
        if exclude is None:
            exclude = []

        # Construct a reference hash, which is useful to know of it's
        # safe to copy a file without checksum or not
        try:
            ref_hash = dict((
                (item.path, item)
                for item in self.list_files(ref_dir)
                if item.mode[0] != 'd'))
        except (CommandFailedException, RsyncListFilesFailure) as e:
            # Here we set ref_hash to None, thus disable the code that marks as
            # "safe matching" those destination files with different time or
            # size, even if newer than "safe_horizon". As a result, all files
            # newer than "safe_horizon" will be checked through checksums.
            ref_hash = None
            _logger.error(
                "Unable to retrieve reference directory file list. "
                "Using only source file information to decide which files"
                " need to be copied with checksums enabled: %s" % e)

        for item in self.list_files(src_dir):
            # Item should not be processed if present in the exclude
            # list
            if FileBasedCopyController._check_exclude(item, exclude):
                continue

            # If item is a directory, we only need to save it
            if item.mode[0] == 'd':
                dir_list.append(item)
                continue

            # If source item is older than safe_horizon,
            # add it to the safe list
            if self.safe_horizon and item.date < self.safe_horizon:
                safe_items.append(item)
                continue

            # If ref_hash is None, it means we failed to retrieve the
            # destination file list. We assume the only safe way is to
            # check every file that is older than safe_horizon
            if ref_hash is None:
                check_items.append(item)
                continue

            # If source file differs by time or size from the matching
            # destination, it is safe to skip checksum check here.
            dst_item = ref_hash.get(item.path, None)
            if (dst_item is None or
                    dst_item.size != item.size or
                    dst_item.date != item.date):

                safe_items.append(item)
                continue

            # All remaining files must be copied with checksum enabled
            check_items.append(item)

        # Create all the required directories
        self._create_directories(dst_dir, dir_list)

        # Store the list inside the instance, as these will be useful to
        # create the jobs
        self.dir_info.append({
            'tbs': tbs,
            'src': src_dir,
            'dst': dst_dir,
            'safe_items': safe_items,
            'check_items': check_items,
            'ref': ref_dir,
            'bwlimit': bwlimit
        })

    @staticmethod
    def _create_directories(dst, dir_list):
        """
        Create a list of directories, every directory's name is relative to
        starting from `dst`
        :param str dst: the destination directory
        :param list[barman.command_wrappers.Rsync.FileItem] dir_list: the
          names of the directories
        """
        # TODO: Use barman.fs.UnixLocalCommand / UnixRemoteCommand
        for item in dir_list:
            mkpath(os.path.join(dst, item.path))

    @abstractmethod
    def list_files(self, src):
        """
        List all files in the server directory

        :param str src: The directory to read
        :return list[barman.command_wrappers.Rsync.FileItem]: an iterable
          representing all the files
        """

    @abstractmethod
    def create_jobs(self, src, dst, filelist,
                    bwlimit=None, checksum=False, ref_dir=None):
        """
        Create the copy jobs for a set of files

        :param str src: the source directory
        :param str dst: the destination directory
        :param list[str] filelist: the list of files to copy
        :param str bwlimit: the bandwidth limit to apply
        :param bool checksum: true if the files must be copy with checksum
        :param str ref_dir: the reference directory if needed
        """

    def _reuse_dir(self, previous_backup_info, oid=None):
        """
        If reuse_backup is 'copy' or 'link', builds the path of the directory
        to reuse, otherwise always returns None.

        If oid is None, it returns the full path of PGDATA directory of
        the previous_backup otherwise it returns the path to the specified
        tablespace using it's oid.

        :param barman.infofile.BackupInfo previous_backup_info: backup to be
            reused
        :param str oid: oid of the tablespace to be reused
        :returns: a string containing the local path with data to be reused
            or None
        :rtype: str|None
        """
        if self.config.reuse_backup in ('copy', 'link') and \
           previous_backup_info is not None:
            try:
                return previous_backup_info.get_data_directory(oid)
            except ValueError:
                return None

    @staticmethod
    def _check_exclude(item, exclude_list):
        """
        Check if a file in the `item` list should be excluded according
        to the `exclude_list`.

        :param barman.command_wrappers.Rsync.FileItem item: list of
           items to be checked
        :param list[str] exclude_list: list of path prefixes to exclude
        :return bool: True if the item should be excluded, False if not
        """
        for prefix in exclude_list:
            if item.path.startswith(prefix):
                return True

        return False


class RsyncCopyController(FileBasedCopyController):
    """
    This copy controller implements primitives using the rsync command
    """

    def __init__(self, path, ssh, ssh_options, network_compression,
                 reuse_backup, safe_horizon, workers):

        super(RsyncCopyController, self).__init__(path=path,
                                                  reuse_backup=reuse_backup,
                                                  safe_horizon=safe_horizon)
        self.ssh_command = ssh
        self.ssh_options = ssh_options
        self.network_compression = network_compression
        self.workers = workers

    def list_files(self, src):
        rsync = Rsync(ssh=self.ssh_command,
                      ssh_options=self.ssh_options,
                      path=self.path)
        return rsync.list_files('%s/' % src)

    def create_jobs(self, src, dst, filelist,
                    bwlimit=None, checksum=False, ref_dir=None):
        items = [item.path for item in filelist]
        for item in filelist:
            job = RsyncCopyJob(
                items=items,
                src=os.path.join(src, item.path),
                dst=os.path.join(dst, item.path),
                ssh_command=self.ssh_command,
                ssh_options=self.ssh_options,
                bwlimit=bwlimit,
                path=self.path,
                reuse_args=self._reuse_args(ref_dir),
                network_compression=self.network_compression,
                checksum=checksum
            )
            self.jobs_list.append(job)

    def _reuse_args(self, reuse_dir):
        """
        If reuse_backup is 'copy' or 'link', build the rsync option to enable
        the reuse, otherwise returns an empty list

        :param str reuse_dir: the local path with data to be reused or None
        :returns: list of argument for rsync call for incremental backup
            or empty list.
        :rtype: list(str)
        """
        if self.reuse_backup in ('copy', 'link') and \
           reuse_dir is not None:
            return ['--%s-dest=%s' % (self.reuse_backup, reuse_dir)]
        else:
            return []


class RsyncCopyJob(object):
    def __init__(self, items, src, dst, ssh_command, reuse_args, ssh_options,
                 bwlimit, path, network_compression, checksum=True):
        """
        :param list[str] items: files to copy
        :param str src: source directory
        :param str dst: destination directory
        :param str ssh_command: ssh command
        :param str reuse_args: parameters needed to reuse the current data
        :param str ssh_options: ssh options
        :param str bwlimit: bandwidth limit options
        :param str path: path where to find the ssh command
        :param str network_compression: network compression to use
        :param bool checksum: if true, copy files using the checksum
        """

        self.bwlimit = bwlimit
        self.checksum = checksum
        self.dst = dst
        self.items = items
        self.path = path
        self.reuse_args = reuse_args
        self.src = src
        self.ssh_command = ssh_command
        self.ssh_options = ssh_options
        self.network_compression = network_compression

    def execute(self):
        try:
            rsync = RsyncPgData(
                path=self.path,
                ssh=self.ssh_command,
                ssh_options=self.ssh_options,
                args=self.reuse_args,
                bwlimit=self.bwlimit,
                network_compression=self.network_compression,
                check=self.checksum)
            # rsync.from_file_list(self.items, '%s/' % self.src, self.dst)
            rsync._rsync_ignore_vanished_files(self.src, self.dst)
        except CommandFailedException as e:
            msg = "data transfer failure on directory '%s'" % \
                  self.dst
            raise DataTransferFailure.from_command_error(
                'rsync', e, msg)
