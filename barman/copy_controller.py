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
TODO
"""
import logging
import shutil
import tempfile
import os.path
from abc import abstractmethod, ABCMeta
from multiprocessing import Pool

from barman.remote_status import RemoteStatusMixin

from barman.command_wrappers import Rsync, RsyncPgData
from barman.exceptions import CommandFailedException, RsyncListFilesFailure, \
    DataTransferFailure
from barman.utils import mkpath, with_metaclass

_logger = logging.getLogger(__name__)


def execute_job(job):
    """

    :param job:
    :return:
    """
    return job.execute()


class CopyController(with_metaclass(ABCMeta, RemoteStatusMixin)):
    """
    TODO
    """
    config = None

    def __init__(self, executor):
        """

        :param barman.config.ServerConfig config:
        """
        super(CopyController, self).__init__()
        self.executor = executor
        self.config = executor.config
        self.jobs_list = []
        self.current_action = None

    @abstractmethod
    def start_copy(self, src, dst, **kwargs):
        pass

    @abstractmethod
    def backup_copy(self, previous_backup, backup_info):
        pass

    @abstractmethod
    def stop_copy(self):
        pass


class FileCopy(CopyController):
    def stop_copy(self):
        pass

    def fetch_remote_status(self):
        pass

    def start_copy(self, src, dst, **kwargs):
        pass

    def backup_copy(self, previous_backup, backup_info):
        """
        :param barman.infofile.BackupInfo previous_backup: backup information
        :param barman.infofile.BackupInfo backup_info: backup information
        """
        # List of paths to be ignored by Rsync
        exclude_and_protect = []

        if previous_backup:
            # safe_horizon is a tz-aware timestamp because BackupInfo class
            # ensures it
            safe_horizon = previous_backup.begin_time
        else:
            # If no previous backup is present, safe_horizon is set to None
            safe_horizon = None
        self.jobs_list = {}
        # Copy tablespaces applying bwlimit when necessary
        if backup_info.tablespaces:
            # Copy a tablespace at a time
            for tablespace in backup_info.tablespaces:
                # If the tablespace location is inside the data directory,
                # exclude and protect it from being copied twice during
                # the data directory copy
                if tablespace.location.startswith(backup_info.pgdata):
                    exclude_and_protect.append(
                        tablespace.location[len(backup_info.pgdata):])
                # Make sure the destination directory exists in order for
                # smart copy to detect that no file is present there
                tablespace_dest = backup_info.get_data_directory(
                    tablespace.oid)
                mkpath(tablespace_dest)
                # Exclude and protect the tablespace from being copied again
                # during the data directory copy
                exclude_and_protect.append("/pg_tblspc/%s" % tablespace.oid)
                # Copy the backup using smart_copy trying to reuse the
                # tablespace of the previous backup if incremental is active
                ref_dir = self._reuse_dir(previous_backup, tablespace.oid)

                self.jobs_list[tablespace.oid] = self.start_copy(
                    ':%s/' % tablespace.location,
                    tablespace_dest,
                    ref=ref_dir,
                    tbs=tablespace.name,
                    safe_horizon=safe_horizon
                )

        backup_dest = backup_info.get_data_directory()
        mkpath(backup_dest)
        ref_dir = self._reuse_dir(previous_backup)
        self.jobs_list['pg_data'] = self.start_copy(
            ':%s/' % backup_info.pgdata,
            backup_dest,
            ref=ref_dir,
            tbs='pg_data',
            safe_horizon=safe_horizon
        )

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
        if self.config.reuse_backup in ('copy', 'link') \
                and previous_backup_info is not None:
            try:
                return previous_backup_info.get_data_directory(oid)
            except ValueError:
                return None

    def _reuse_args(self, reuse_dir):
        """
        If reuse_backup is 'copy' or 'link', build the rsync option to enable
        the reuse, otherwise returns an empty list

        :param str reuse_dir: the local path with data to be reused or None
        :returns: list of argument for rsync call for incremental backup
            or empty list.
        :rtype: list(str)
        """
        if self.config.reuse_backup in ('copy', 'link')\
                and reuse_dir is not None:
            return ['--%s-dest=%s' % (self.config.reuse_backup, reuse_dir)]
        else:
            return []


class RsyncCopy(FileCopy):
    def fetch_remote_status(self):
        pass

    def stop_copy(self):
        pass

    def _do_copy(self):
        """
        Pools copy processes
        """
        pool = Pool(processes=self.config.rsync_workers)
        execute_method = execute_job
        try:
            for x in self.jobs_list.values():
                for result in pool.imap_unordered(execute_method, x):
                    _logger.debug(result)

        except DataTransferFailure as e:
            _logger.exception(e)
            raise

    def backup_copy(self, previous_backup, backup_info, **kwargs):
        """
        """
        super(RsyncCopy, self).backup_copy(previous_backup, backup_info)
        self._do_copy()
        self.stop_copy()

    def start_copy(self, src, dst, **kwargs):
        """
        Set up copy environment and prepare files
        """

        result = []
        ref = kwargs.get('ref', None)
        tbs = kwargs.get('tbs', None)
        safe_horizon = kwargs.get('safe_horizon', None)

        _logger.info("Smart copy: %r -> %r (ref: %r, safe before %r)",
                     src, dst, ref, safe_horizon)

        # If reference is not set we use dst as reference path
        if ref is None:
            ref = dst

        # Make sure the ref path ends with a '/' or rsync will add the
        # last path component to all the returned items during listing
        if ref[-1] != '/':
            ref += '/'

        # Use the --no-human-readable option to avoid digit groupings
        # in "size" field with rsync >= 3.1.0.
        # Ref: http://ftp.samba.org/pub/rsync/src/rsync-3.1.0-NEWS
        rsync = Rsync(ssh=self.executor.ssh_command,
                      ssh_options=self.executor.ssh_options,
                      path=self.config.path_prefix)

        # Build a hash containing all files present on reference directory.
        # Directories are not included
        _logger.info("Smart copy step 1/4: preparation")
        try:
            ref_hash = dict((
                (item.path, item)
                for item in rsync.list_files(ref)
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

        # We need a temporary directory to store the files containing the lists
        # we are building in order to instruct rsync about which files need to
        # be copied at different stages
        temp_dir = tempfile.mkdtemp(prefix='barman-')
        reuse = self.executor._reuse_args(ref)
        try:
            # The 'dir.list' file will contain every directory in the
            # source tree
            dir_list = open(os.path.join(temp_dir, 'dir.list'), 'w+')
            # The 'protect.list' file will contain a filter rule to protect
            # each file present in the source tree. It will be used during
            # the first phase to delete all the extra files on destination.
            exclude_and_protect_filter = open(
                os.path.join(temp_dir, 'exclude_and_protect.filter'), 'w+')
            compression = self.config.network_compression
            for item in rsync.list_files(src):
                # If item is a directory, we only need to save it in 'dir.list'
                if item.mode[0] == 'd':
                    dir_list.write(item.path + '\n')
                    continue
                # Add every file in the source path to the list of files
                # to be protected from deletion ('exclude_and_protect.filter')
                exclude_and_protect_filter.write('P ' + item.path + '\n')
                exclude_and_protect_filter.write('- ' + item.path + '\n')

                # If source item is older than safe_horizon,
                # add it to 'safe.list'
                if safe_horizon and item.date < safe_horizon:
                    result.append(
                        RsyncCopyJob(
                            item_path=item.path, src=src, dst=dst,
                            ssh_command=self.executor.ssh_command,
                            ssh_options=self.executor.ssh_options,
                            bwlimit=self.config.bandwidth_limit,
                            path=self.config.path_prefix,
                            safe_horizon=safe_horizon,
                            reuse_args=reuse,
                            network_compression=compression,
                            tbs=tbs,
                            ref_dir=ref,
                            checksum=False
                        )
                    )
                    continue

                # If ref_hash is None, it means we failed to retrieve the
                # destination file list. We assume the only safe way is to
                # check every file that is older than safe_horizon
                if ref_hash is None:
                    result.append(
                        RsyncCopyJob(
                            item_path=item.path, src=src, dst=dst,
                            ssh_command=self.executor.ssh_command,
                            ssh_options=self.executor.ssh_options,
                            bwlimit=self.config.bandwidth_limit,
                            path=self.config.path_prefix,
                            safe_horizon=safe_horizon,
                            reuse_args=reuse,
                            network_compression=compression,
                            tbs=tbs,
                            ref_dir=ref,
                            checksum=True
                        )
                    )
                    continue

                # If source file differs by time or size from the matching
                # destination, rsync will discover the difference in any case.
                # It is then safe to skip checksum check here.
                dst_item = ref_hash.get(item.path, None)
                if dst_item is None \
                        or dst_item.size != item.size \
                        or dst_item.date != item.date:
                    result.append(
                        RsyncCopyJob(
                            item_path=item.path, src=src, dst=dst,
                            ssh_command=self.executor.ssh_command,
                            ssh_options=self.executor.ssh_options,
                            bwlimit=self.config.bandwidth_limit,
                            path=self.config.path_prefix,
                            safe_horizon=safe_horizon,
                            reuse_args=reuse,
                            network_compression=compression,
                            tbs=tbs,
                            ref_dir=ref,
                            checksum=False
                        )
                    )
                    continue

                # All remaining files must be checked with checksums enabled
                result.append(
                    RsyncCopyJob(
                        item_path=item.path,
                        src=src,
                        dst=dst,
                        ssh_command=self.executor.ssh_command,
                        ssh_options=self.executor.ssh_options,
                        bwlimit=self.config.bandwidth_limit,
                        path=self.config.path_prefix,
                        safe_horizon=safe_horizon,
                        reuse_args=reuse,
                        network_compression=compression,
                        tbs=tbs,
                        ref_dir=ref,
                        checksum=True
                    )
                )

            exclude_and_protect_filter.close()
            dir_list.close()
            try:
                rsync._rsync_ignore_vanished_files(
                    '--recursive',
                    '--delete', '--itemize-changes', '--itemize-changes',
                    '--files-from=%s' % dir_list.name,
                    '--filter', 'merge %s' % exclude_and_protect_filter.name,
                    src, dst,
                    check=True)
            except Exception as e:
                _logger.debug(e)
        finally:
            shutil.rmtree(temp_dir)

        return result


class Job(object):
    def execute(self):
        pass


class RsyncCopyJob(Job):
    def __init__(self, item_path, src, dst, ssh_command, reuse_args,
                 safe_horizon, ssh_options, bwlimit, path,
                 network_compression, ref_dir, tbs, checksum=True):
        """
        :param barman.command_wrappers.Rsync.FileItem item:
        :param str src:
        :param str dst:
        :param bool checksum:
        """

        full_src = os.path.join(src, item_path)
        full_dst = os.path.join(dst, item_path)

        self.bwlimit = bwlimit
        self.checksum = checksum
        self.dst = full_dst
        self.path = path
        self.reuse_args = reuse_args
        self.safe_horizon = safe_horizon
        self.src = full_src
        self.ssh_command = ssh_command
        self.ssh_options = ssh_options
        self.ref_dir = ref_dir
        self.tbs = tbs
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
            rsync._rsync_ignore_vanished_files(
                self.src, self.dst)
        except CommandFailedException as e:
            msg = "data transfer failure on directory '%s'" % \
                  self.dst
            raise DataTransferFailure.from_command_error(
                'rsync', e, msg)
