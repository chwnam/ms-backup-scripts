import sys
from datetime import datetime
from json import loads
from os import access, X_OK, chdir, getenv, R_OK, getcwd, remove, system, listdir, mkdir
from os.path import basename, dirname, exists, expanduser, isdir, isfile, join
from re import compile
from shutil import which, copy, rmtree
from subprocess import run, PIPE
from sys import exit, stderr
from tempfile import gettempdir
from typing import Tuple, List

from dotenv import load_dotenv


class EnvCheck(object):
    def __init__(self):
        self.conf_remote_path = ''
        self.conf_targets = []
        self.conf_capacity = 0

        self.gzip = ''
        self.mysqldump = ''
        self.rclone = ''
        self.tar = ''
        self.wp_cli = ''

        self.wp_root = ''
        self.wp_remote_path = ''
        self.wp_remote_capacity = 0

    def test_gzip(self) -> None:
        self.gzip = self.which('gzip')
        if not self.gzip:
            raise FileNotFoundError('gzip')

    def test_mysqldump(self) -> None:
        self.mysqldump = self.which('mysqldump')
        if not self.mysqldump:
            raise FileNotFoundError('mysqldump')

    def test_rclone(self) -> None:
        self.rclone = self.which('rclone')
        if not self.rclone:
            raise FileNotFoundError('rclone')

    def test_tar(self) -> None:
        self.tar = self.which('tar')
        if not self.tar:
            raise FileNotFoundError('tar')

    def test_wp_cli(self) -> None:
        self.wp_cli = self.which('wp')
        if not self.wp_cli:
            self.wp_cli = self.which('wp-cli.phar')
        if not self.wp_cli:
            self.wp_cli = self.which('wp-cli')
        if not self.wp_cli:
            self.wp_cli = self.which('wp_cli')
        if not self.wp_cli:
            raise FileNotFoundError('wp')

    def test_wp_root(self, wp_root: str) -> None:
        if not self.wp_root:
            wp_config = join(wp_root, 'wp-config.php')
            self.test_dir(wp_root)
            self.test_file(wp_config)
            self.wp_root = wp_root

    @staticmethod
    def test_dir(path: str | None) -> None:
        if exists(path) and isdir(path) and access(path, X_OK):
            return
        raise FileNotFoundError(path)

    @staticmethod
    def test_file(path: str | None) -> None:
        if exists(path) and isfile(path) and access(path, R_OK):
            return
        raise FileNotFoundError(path)

    @staticmethod
    def which(cmd: str) -> str:
        result = which(cmd)
        return result if result else ''

    @staticmethod
    def exec(cmd: str) -> Tuple[int, str]:
        result = run(cmd.split(' '), stdout=PIPE)
        stdout = result.stdout.decode('utf-8')
        code = result.returncode
        return code, stdout


def backup_wordpress() -> None:
    env = initialize()

    # Get file names.
    tempdir = gettempdir()
    now = datetime.now().strftime('%Y%m%d')
    snap_path = join(tempdir, 'wpsnap_' + now + '.tar.gz')
    dump_path = join(tempdir, 'wpdb_' + now + '.sql.gz')

    # Store the old cwd.
    cwd = getcwd()

    # Create snapshot file.
    chdir(dirname(env.wp_root))

    print('Creating tarball of `' + env.wp_root + '` to `' + snap_path + '` ...')
    command = '{} -czf {} {}'.format(env.tar, snap_path, basename(env.wp_root))
    env.exec(command)

    # Create database dump using WP-CLI, and gzip
    chdir(env.wp_root)

    print('Creating database dump to `' + dump_path + '` ...')
    command = '{} db export - | {} -9 > {}'.format(env.wp_cli, env.gzip, dump_path)
    system(command)

    # rclone to onedrive
    print('Upload to OneDrive ...')

    # Snapshot
    destination = join(env.wp_remote_path, basename(snap_path))
    command = '{} copyto {} {}'.format(env.rclone, snap_path, destination)
    env.exec(command)

    # DB
    destination = join(env.wp_remote_path, basename(dump_path))
    command = '{} copyto {} {}'.format(env.rclone, dump_path, destination)
    env.exec(command)

    # Remove the backup file.
    remove(snap_path)
    remove(dump_path)

    # Rollback
    chdir(cwd)


def backup_configs() -> None:
    env = initialize()

    # Get file names.
    tempdir = gettempdir()
    dest_dir = join(tempdir, 'conf')
    mkdir(dest_dir)

    with open(join(dest_dir, 'README.txt'), 'w') as fp:
        fp.write("Configuration Backup\n")
        fp.write("====================\n")
        fp.write("Backup date: " + datetime.now().strftime('%Y-%m-%d') + '\n')

        for src in env.conf_targets:
            src = expanduser(src)
            if not src or not exists(src):
                continue

            if isfile(src) and access(src, R_OK):
                print('Copying {}'.format(src))
                copy(src, join(dest_dir, basename(src)))
                fp.write(src + '\n')

        fp.write("E.O.D.\n")

    cwd = getcwd()
    chdir(tempdir)
    archive_path = 'conf-' + datetime.now().strftime('%Y%m%d') + '.tar.gz'

    print('Creating tarball of configurations ...')
    command = '{} -czf {} {}'.format(env.tar, archive_path, basename(dest_dir))
    env.exec(command)

    print('Upload to OneDrive ...')
    destination = join(env.conf_remote_path, archive_path)
    command = '{} copyto {} {}'.format(env.rclone, archive_path, destination)
    env.exec(command)

    chdir(cwd)
    remove(join(tempdir, archive_path))
    rmtree(dest_dir)


def limit_conf_backup() -> None:
    env = initialize()
    exp = compile(r'conf-\d{8}\.tar\.gz$')

    command = '{} lsjson {}'.format(env.rclone, env.wp_remote_path)
    code, output = env.exec(command)

    files: List[Tuple[str, datetime]] = []

    if 0 != code:
        print('The command returned non-zero value: ' + command, file=stderr)
        exit(1)

    items = loads(output)

    for item in items:
        is_dir = item['IsDir']
        mime_type = item['MimeType']
        path = item['Path']
        matched = exp.match(path)

        if is_dir or 'application/x-gzip' != mime_type or not matched:
            continue

        mtime = datetime.fromisoformat(item['ModTime'].replace('Z', '+00:00'))
        files.append((path, mtime))

    # Sort by mtime
    files.sort(key=lambda x: x[1], reverse=True)

    to_remove: List[str] = []

    if env.conf_capacity < len(files):
        exceeded = files[env.conf_capacity:]
        for item in exceeded:
            to_remove.append(item[0])

    if to_remove:
        print('There are more files than capacity. Let\'s remove them.')
        for item in to_remove:
            command = '{} delete {}'.format(env.rclone, join(env.wp_remote_path, item))
            env.exec(command)
    else:
        print('Capacity not exceeded. Okay then.')


def limit_wp_backup() -> None:
    env = initialize()
    exp = compile(r'^wp(db|snap)_\d{8}\.(?:tar|sql)\.gz$')

    command = '{} lsjson {}'.format(env.rclone, env.wp_remote_path)
    code, output = env.exec(command)

    snaps: List[Tuple[str, datetime]] = []
    dbs: List[Tuple[str, datetime]] = []

    if 0 != code:
        print('The command returned non-zero value: ' + command, file=stderr)
        exit(1)

    items = loads(output)

    for item in items:
        is_dir = item['IsDir']
        mime_type = item['MimeType']
        path = item['Path']
        matched = exp.match(path)

        if is_dir or 'application/x-gzip' != mime_type or not matched:
            continue

        # db or snap
        match_type = matched.group(1)
        mtime = datetime.fromisoformat(item['ModTime'].replace('Z', '+00:00'))

        if 'db' == match_type:
            dbs.append((path, mtime))
        else:
            snaps.append((path, mtime))

    # Sort by mtime
    dbs.sort(key=lambda x: x[1], reverse=True)
    snaps.sort(key=lambda x: x[1], reverse=True)

    to_remove: List[str] = []

    if env.wp_remote_capacity < len(dbs):
        exceeded = dbs[env.wp_remote_capacity:]
        for item in exceeded:
            to_remove.append(item[0])

    if env.wp_remote_capacity < len(snaps):
        exceeded = snaps[env.wp_remote_capacity:]
        for item in exceeded:
            to_remove.append(item[0])

    if to_remove:
        print('There are more files than capacity. Let\'s remove them.')
        for item in to_remove:
            command = '{} delete {}'.format(env.rclone, join(env.wp_remote_path, item))
            env.exec(command)
    else:
        print('Capacity not exceeded. Okay then.')


def initialize() -> EnvCheck:
    env_check = EnvCheck()

    try:
        env_check.conf_remote_path = getenv('CONF_REMOTE_PATH')

        for target in getenv('CONF_TARGETS').split('\n'):
            target = target.strip()
            if target:
                env_check.conf_targets.append(target)

        env_check.conf_capacity = int(getenv('CONF_CAPACITY')) if getenv('CONF_CAPACITY') else 5

        env_check.test_gzip()
        env_check.test_mysqldump()
        env_check.test_rclone()
        env_check.test_tar()
        env_check.test_wp_cli()

        env_check.test_wp_root(expanduser(getenv('WP_ROOT')))
        env_check.wp_remote_path = getenv('WP_REMOTE_PATH')

        env_check.wp_remote_capacity = int(getenv('WP_REMOTE_CAPACITY')) if getenv('WP_REMOTE_CAPACITY') else 5
    except FileNotFoundError as err:
        print(basename(err.args[0]) + ' not found!')
        exit(1)

    return env_check


if '__main__' == __name__:
    load_dotenv()

    backup_wordpress()
    limit_wp_backup()

    backup_configs()
    limit_conf_backup()
