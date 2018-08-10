from functools import wraps
import os
import tempfile
import s3_sync.verify
from . import TestCloudSyncBase


def swift_is_unchanged(func):
    @wraps(func)
    def wrapper(test):
        before = test.get_swift_tree(test.swift_dst)
        func(test)
        test.assertEqual(before, test.get_swift_tree(test.swift_dst))
    return wrapper


def s3_is_unchanged(func):
    @wraps(func)
    def wrapper(test):
        before = test.get_s3_tree()
        func(test)
        test.assertEqual(before, test.get_s3_tree())
    return wrapper


class TestVerify(TestCloudSyncBase):
    def setUp(self):
        self.swift_container = self.s3_bucket = None
        for container in self.test_conf['containers']:
            if container['protocol'] == 'swift':
                self.swift_container = container['aws_bucket']
            else:
                self.s3_bucket = container['aws_bucket']
            if self.swift_container and self.s3_bucket:
                break
        fd, self.tmpfile = tempfile.mkstemp()
        os.close(fd)

    def tearDown(self):
        os.unlink(self.tmpfile)

    @swift_is_unchanged
    def test_swift_no_container(self):
        with open(self.tmpfile, 'w') as f:
            f.write(self.SWIFT_CREDS['dst']['key'])

        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.tmpfile,
        ]))

    @swift_is_unchanged
    def test_swift_single_container(self):
        with open(self.tmpfile, 'w') as f:
            f.write(self.SWIFT_CREDS['dst']['key'])

        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.tmpfile,
            '--bucket=' + self.swift_container,
        ]))

    @swift_is_unchanged
    def test_swift_admin_cross_account(self):
        with open(self.tmpfile, 'w') as f:
            f.write(self.SWIFT_CREDS['admin']['key'])

        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['admin']['user'],
            '--password=' + self.tmpfile,
            '--account=AUTH_\xd8\xaaacct2',
            '--bucket=' + self.swift_container,
        ]))

    @swift_is_unchanged
    def test_swift_admin_wrong_account(self):
        # Note: "wrong account", here, means an account that doesn't have the
        # specified bucket.
        # ...which is basically the same as `test_swift_bad_container` but with
        # an admin user.
        with open(self.tmpfile, 'w') as f:
            f.write(self.SWIFT_CREDS['admin']['key'])

        actual = s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['admin']['user'],
            '--password=' + self.tmpfile,
            '--account=AUTH_test',
            '--bucket=' + self.swift_container,
        ])
        self.assertIn('404 Not Found', actual)
        self.assertTrue(actual.startswith(
            'Unexpected error validating credentials: Object PUT failed: '))

    @swift_is_unchanged
    def test_swift_all_containers(self):
        with open(self.tmpfile, 'w') as f:
            f.write(self.SWIFT_CREDS['dst']['key'])

        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.tmpfile,
            '--bucket=/*',
        ]))

    @swift_is_unchanged
    def test_swift_bad_creds(self):
        with open(self.tmpfile, 'w') as f:
            f.write('not-the-password')

        msg = ('Invalid credentials. Please check the Access Key ID and '
               'Secret Access Key.')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.tmpfile,
            '--bucket=' + self.swift_container,
        ]))

    @swift_is_unchanged
    def test_swift_bad_container(self):
        with open(self.tmpfile, 'w') as f:
            f.write(self.SWIFT_CREDS['dst']['key'])

        actual = s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.tmpfile,
            '--bucket=does-not-exist',
        ])
        self.assertIn('404 Not Found', actual)
        self.assertTrue(actual.startswith(
            'Unexpected error validating credentials: Object PUT failed: '))

    @s3_is_unchanged
    def test_s3_no_bucket(self):
        with open(self.tmpfile, 'w') as f:
            f.write(self.S3_CREDS['key'])

        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.tmpfile,
        ]))

    @s3_is_unchanged
    def test_s3_single_bucket(self):
        with open(self.tmpfile, 'w') as f:
            f.write(self.S3_CREDS['key'])

        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.tmpfile,
            '--bucket=' + self.s3_bucket,
        ]))

    @s3_is_unchanged
    def test_s3_bad_creds(self):
        with open(self.tmpfile, 'w') as f:
            f.write('not-the-password')

        msg = ('Invalid credentials. Please check the Access Key ID and '
               'Secret Access Key.')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.tmpfile,
            '--bucket=' + self.s3_bucket,
        ]))

    @s3_is_unchanged
    def test_s3_bad_bucket(self):
        with open(self.tmpfile, 'w') as f:
            f.write(self.S3_CREDS['key'])

        msg = ("Unexpected error validating credentials: 'The specified "
               "bucket does not exist'")
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.tmpfile,
            '--bucket=does-not-exist',
        ]))
