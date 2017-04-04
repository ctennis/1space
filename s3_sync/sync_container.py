import eventlet
eventlet.patcher.monkey_patch(all=True)

import boto3
import botocore.exceptions
from botocore.handlers import conditionally_calculate_md5
import eventlet
import hashlib
import json
import logging
import os
import os.path
import traceback

import container_crawler.base_sync
from swift.common.utils import FileLikeIter
from .utils import (convert_to_s3_headers, FileWrapper, SLOFileWrapper,
                    is_object_meta_synced, get_slo_etag, check_slo,
                    SLO_ETAG_FIELD)


class SyncContainer(container_crawler.base_sync.BaseSync):
    # S3 prefix space: 6 16 digit characters
    PREFIX_LEN = 6
    PREFIX_SPACE = 16**PREFIX_LEN
    BOTO_CONN_POOL_SIZE = 10
    SLO_WORKERS = 10
    SLO_QUEUE_SIZE = 100
    MB = 1024*1024
    GB = 1024*MB
    MIN_PART_SIZE = 5*MB
    MAX_PART_SIZE = 5*GB
    MAX_PARTS = 10000
    GOOGLE_API = 'https://storage.googleapis.com'
    CLOUD_SYNC_VERSION = '5.0'
    GOOGLE_UA_STRING = 'CloudSync/%s (GPN:SwiftStack)' % CLOUD_SYNC_VERSION

    class HttpClientPoolEntry(object):
        def __init__(self, client):
            self.semaphore = eventlet.semaphore.Semaphore(
                SyncContainer.BOTO_CONN_POOL_SIZE)
            self.client = client

        def acquire(self):
            return self.semaphore.acquire(blocking=True)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.semaphore.release()
            return False

    class HttpClientPool(object):
        def __init__(self, client_factory, max_conns):
            self.get_semaphore = eventlet.semaphore.Semaphore(max_conns)
            self.client_pool = self._create_pool(client_factory, max_conns)

        def _create_pool(self, client_factory, max_conns):
            clients = max_conns / SyncContainer.BOTO_CONN_POOL_SIZE
            if max_conns % SyncContainer.BOTO_CONN_POOL_SIZE:
                clients += 1
            return [SyncContainer.HttpClientPoolEntry(client_factory())
                    for _ in range(0, clients)]

        def get_client(self):
            # SLO uploads may exhaust the client pool and we will need to wait
            # for connections
            with self.get_semaphore:
                # we are guaranteed that there is an open connection we can use
                for client in self.client_pool:
                    if client.acquire():
                        return client

    def __init__(self, status_dir, sync_settings, max_conns=10):
        super(SyncContainer, self).__init__(status_dir, sync_settings)
        self.account = sync_settings['account']
        self.container = sync_settings['container']
        self._status_dir = status_dir
        self._status_file = os.path.join(self._status_dir, self.account,
                                         self.container)
        self._status_account_dir = os.path.join(self._status_dir, self.account)
        self._init_pool(sync_settings, max_conns)
        self.logger = logging.getLogger('s3-sync')

    def _init_pool(self, settings, max_conns):
        self.aws_bucket = settings['aws_bucket']
        self.client_pool = self.HttpClientPool(
            self._get_boto_client_factory(settings), max_conns)

    def _get_boto_client_factory(self, settings):
        aws_identity = settings['aws_identity']
        aws_secret = settings['aws_secret']
        # assumes local swift-proxy
        aws_endpoint = settings.get('aws_endpoint', None)
        self._google = aws_endpoint == self.GOOGLE_API

        boto_session = boto3.session.Session(
            aws_access_key_id=aws_identity,
            aws_secret_access_key=aws_secret)
        if not aws_endpoint or aws_endpoint.endswith('amazonaws.com'):
            # We always use v4 signer with Amazon, as it will support all
            # regions.
            boto_config = boto3.session.Config(signature_version='s3v4',
                                               s3={'aws_chunked': True})
        else:
            # For the other providers, we default to v2 signer, as a lot of
            # them don't support v4 (e.g. Google)
            boto_config = boto3.session.Config(s3={'addressing_style': 'path'})
            if self._google:
                boto_config.user_agent = "%s %s" % (
                    self.GOOGLE_UA_STRING, boto_session._session.user_agent())

        def boto_client_factory():
            s3_client = boto_session.client('s3',
                                            endpoint_url=aws_endpoint,
                                            config=boto_config)
            # Remove the Content-MD5 computation as we will supply the MD5
            # header ourselves
            s3_client.meta.events.unregister('before-call.s3.PutObject',
                                             conditionally_calculate_md5)
            s3_client.meta.events.unregister('before-call.s3.UploadPart',
                                             conditionally_calculate_md5)
            return s3_client
        return boto_client_factory

    def get_last_row(self, db_id):
        if not os.path.exists(self._status_file):
            return 0
        with open(self._status_file) as f:
            try:
                status = json.load(f)
                # First iteration did not include the bucket and DB ID
                if 'last_row' in status:
                    return status['last_row']
                if db_id in status:
                    entry = status[db_id]
                    if entry['aws_bucket'] == self.aws_bucket:
                        return entry['last_row']
                    else:
                        return 0
                return 0
            except ValueError:
                return 0

    def save_last_row(self, row, db_id):
        if not os.path.exists(self._status_account_dir):
            os.mkdir(self._status_account_dir)
        if not os.path.exists(self._status_file):
            with open(self._status_file, 'w') as f:
                json.dump({db_id: dict(last_row=row,
                                       aws_bucket=self.aws_bucket)}, f)
                return

        with open(self._status_file, 'r+') as f:
            status = json.load(f)
            # The first version did not include the DB ID and aws_bucket in the
            # status entries
            if 'last_row' in status:
                status = {db_id: dict(last_row=row,
                                      aws_bucket=self.aws_bucket)}
            else:
                status[db_id] = dict(last_row=row,
                                     aws_bucket=self.aws_bucket)
            f.seek(0)
            json.dump(status, f)
            f.truncate()

    def get_s3_name(self, key):
        md5_hash = hashlib.md5('%s/%s' % (
            self.account, self.container)).hexdigest()
        # strip off 0x and L
        prefix = hex(long(md5_hash, 16) % self.PREFIX_SPACE)[2:-1]
        return '%s/%s' % (prefix, self._full_name(key))

    def handle(self, row):
        if row['deleted']:
            self.delete_object(row['name'])
        else:
            self.upload_object(row['name'], row['storage_policy_index'])

    def upload_object(self, swift_key, storage_policy_index):
        s3_key = self.get_s3_name(swift_key)
        try:
            with self.client_pool.get_client() as boto_client:
                s3_client = boto_client.client
                s3_meta = s3_client.head_object(Bucket=self.aws_bucket,
                                                Key=s3_key)
        except botocore.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                s3_meta = None
            else:
                raise e
        # TODO:  Handle large objects. Should we delete segments in S3?
        swift_req_hdrs = {
            'X-Backend-Storage-Policy-Index': storage_policy_index,
            'X-Newest': True
        }

        metadata = self._swift_client.get_object_metadata(
            self.account, self.container, swift_key, headers=swift_req_hdrs)

        self.logger.debug("Metadata: %s" % str(metadata))
        if check_slo(metadata):
            self.upload_slo(swift_key, storage_policy_index, s3_meta)
            return

        if s3_meta and self.check_etag(metadata['etag'], s3_meta['ETag']):
            if is_object_meta_synced(s3_meta['Metadata'], metadata):
                return
            elif not self.in_glacier(s3_meta):
                self.update_metadata(metadata, s3_key)
                return

        wrapper_stream = FileWrapper(self._swift_client,
                                     self.account,
                                     self.container,
                                     swift_key,
                                     swift_req_hdrs)
        self.logger.debug('Uploading %s with meta: %r' % (
            s3_key, wrapper_stream.get_s3_headers()))
        with self.client_pool.get_client() as boto_client:
            s3_client = boto_client.client
            s3_client.put_object(Bucket=self.aws_bucket,
                                 Key=s3_key,
                                 Body=wrapper_stream,
                                 Metadata=wrapper_stream.get_s3_headers(),
                                 ContentLength=len(wrapper_stream))

    def upload_slo(self, swift_key, storage_policy_index, s3_meta):
        # Converts an SLO into a multipart upload. We use the segments as
        # is, for the part sizes.
        # NOTE: If the SLO segment is < 5MB and is not the last segment, the
        # UploadPart call will fail. We need to stitch segments together in
        # that case.
        #
        # For Google Cloud Storage, we will convert the SLO into a single
        # object put, assuming the SLO is < 5TB. If the SLO is > 5TB, we have
        # to fail the upload. With GCS _compose_, we could support larger
        # objects, but defer this work for the time being.
        swift_req_hdrs = {
            'X-Backend-Storage-Policy-Index': storage_policy_index,
            'X-Newest': True
        }
        status, headers, body = self._swift_client.get_object(
            self.account, self.container, swift_key, headers=swift_req_hdrs)
        if status != 200:
            body.close()
            raise RuntimeError('Failed to get the manifest')
        manifest = json.load(FileLikeIter(body))
        body.close()
        self.logger.debug("JSON manifest: %s" % str(manifest))
        s3_key = self.get_s3_name(swift_key)

        if not self._validate_slo_manifest(manifest):
            # We do not raise an exception here -- we should not retry these
            # errors and they will be logged.
            # TODO: When we report statistics, we need to account for permanent
            # failures.
            self.logger.error('Failed to validate the SLO manifest for %s' %
                              self._full_name(swift_key))
            return

        if self._google:
            if s3_meta:
                slo_etag = s3_meta['Metadata'].get(SLO_ETAG_FIELD, None)
                if slo_etag == headers['etag']:
                    if is_object_meta_synced(s3_meta['Metadata'], headers):
                        return
                    self.update_metadata(headers, s3_key)
                    return
            self._upload_google_slo(manifest, headers, s3_key, swift_req_hdrs)
            return

        expected_etag = get_slo_etag(manifest)

        if s3_meta and self.check_etag(expected_etag, s3_meta['ETag']):
            if is_object_meta_synced(s3_meta['Metadata'], headers):
                return
            elif not self.in_glacier(s3_meta):
                self.update_slo_metadata(headers, manifest, s3_key,
                                         swift_req_hdrs)
                return

        self._upload_slo(manifest, headers, s3_key, swift_req_hdrs)

    def _upload_google_slo(self, manifest, metadata, s3_key, req_hdrs):
        slo_wrapper = SLOFileWrapper(
            self._swift_client, self.account, manifest, metadata, req_hdrs)
        with self.client_pool.get_client() as boto_client:
            s3_client = boto_client.client
            s3_client.put_object(Bucket=self.aws_bucket,
                                 Key=s3_key,
                                 Body=slo_wrapper,
                                 Metadata=slo_wrapper.get_s3_headers(),
                                 ContentLength=len(slo_wrapper))

    def _validate_slo_manifest(self, manifest):
        parts = len(manifest)
        if parts > self.MAX_PARTS:
            self.logger.error('Cannot upload a manifest with more than %d '
                              'segments. ' % self.MAX_PARTS)
            return False

        for index, segment in enumerate(manifest):
            if 'bytes' not in segment or 'hash' not in segment:
                # Should never happen
                self.logger.error('SLO segment %s must include size and etag' %
                                  segment['name'])
                return False
            size = int(segment['bytes'])
            if size < self.MIN_PART_SIZE and index < parts - 1:
                self.logger.error('SLO segment %s must be greater than %d MB' %
                                  (segment['name'],
                                   self.MIN_PART_SIZE / self.MB))
                return False
            if size > self.MAX_PART_SIZE:
                self.logger.error('SLO segment %s must be smaller than %d GB' %
                                  (segment['name'],
                                   self.MAX_PART_SIZE / self.GB))
                return False
            if 'range' in segment:
                self.logger.error('Found unsupported "range" parameter for %s '
                                  'segment ' % segment['name'])
                return False
        return True

    def _upload_slo(self, manifest, object_meta, s3_key, req_headers):
        with self.client_pool.get_client() as boto_client:
            s3_client = boto_client.client
            multipart_resp = s3_client.create_multipart_upload(
                Bucket=self.aws_bucket,
                Key=s3_key,
                Metadata=convert_to_s3_headers(object_meta))
        upload_id = multipart_resp['UploadId']

        work_queue = eventlet.queue.Queue(self.SLO_QUEUE_SIZE)
        worker_pool = eventlet.greenpool.GreenPool(self.SLO_WORKERS)
        workers = []
        for _ in range(0, self.SLO_WORKERS):
            workers.append(
                worker_pool.spawn(self._upload_part_worker, upload_id, s3_key,
                                  req_headers, work_queue, len(manifest)))
        for segment_number, segment in enumerate(manifest):
            work_queue.put((segment_number + 1, segment))

        work_queue.join()
        for _ in range(0, self.SLO_WORKERS):
            work_queue.put(None)

        errors = []
        for thread in workers:
            errors += thread.wait()

        # TODO: errors list contains the failed part numbers. We should retry
        # those parts on failure.
        if errors:
            self._abort_upload(s3_key, upload_id)
            raise RuntimeError('Failed to upload an SLO as %s' % s3_key)

        with self.client_pool.get_client() as boto_client:
            s3_client = boto_client.client
            # TODO: Validate the response ETag
            try:
                s3_client.complete_multipart_upload(
                    Bucket=self.aws_bucket,
                    Key=s3_key,
                    MultipartUpload={'Parts': [
                        {'PartNumber': number + 1,
                         'ETag': segment['hash']}
                        for number, segment in enumerate(manifest)]
                    },
                    UploadId=upload_id)
            except:
                self._abort_upload(s3_key, upload_id, client=s3_client)
                raise

    def _abort_upload(self, s3_key, upload_id, client=None):
        if not client:
            with self.client_pool.get_client() as boto_client:
                client = boto_client.client
                client.abort_multipart_upload(
                    Bucket=self.aws_bucket, Key=s3_key, UploadId=upload_id)
        else:
            client.abort_multipart_upload(
                Bucket=self.aws_bucket, Key=s3_key, UploadId=upload_id)

    def _upload_part_worker(self, upload_id, s3_key, req_headers, queue,
                            part_count):
        errors = []
        while True:
            work = queue.get()
            if not work:
                queue.task_done()
                return errors

            try:
                part_number, segment = work
                container, obj = segment['name'].split('/', 2)[1:]
                wrapper = FileWrapper(self._swift_client, self.account,
                                      container, obj, req_headers)

                with self.client_pool.get_client() as boto_client:
                    self.logger.debug('Uploading part %d from %s: %s bytes' % (
                        part_number, self.account + segment['name'],
                        segment['bytes']))
                    s3_client = boto_client.client
                    resp = s3_client.upload_part(
                        Bucket=self.aws_bucket,
                        Body=wrapper,
                        Key=s3_key,
                        ContentLength=len(wrapper),
                        UploadId=upload_id,
                        PartNumber=part_number)
                    if not self.check_etag(segment['hash'], resp['ETag']):
                        self.logger.error('Part %d ETag mismatch (%s): %s %s' %
                                          (part_number,
                                           self.account + segment['name'],
                                           segment['hash'], resp['ETag']))
                        errors.append(part_number)
            except:
                self.logger.error('Failed to upload part %d for %s: %s' % (
                    part_number, self.account + segment['name'],
                    traceback.format_exc()))
                errors.append(part_number)
            finally:
                queue.task_done()

    def delete_object(self, swift_key):
        s3_key = self.get_s3_name(swift_key)
        self.logger.debug('Deleting object %s' % s3_key)
        with self.client_pool.get_client() as boto_client:
            s3_client = boto_client.client
            s3_client.delete_object(Bucket=self.aws_bucket, Key=s3_key)

    def update_slo_metadata(self, swift_meta, manifest, s3_key, req_headers):
        # For large objects, we should use the multipart copy, which means
        # creating a new multipart upload, with copy-parts
        # NOTE: if we ever stich MPU objects, we need to replicate the
        # stitching calculation to get the offset correctly.
        with self.client_pool.get_client() as boto_client:
            s3_client = boto_client.client
            multipart_resp = s3_client.create_multipart_upload(
                Bucket=self.aws_bucket,
                Key=s3_key,
                Metadata=convert_to_s3_headers(swift_meta))

            # The original manifest must match the MPU parts to ensure that
            # ETags match
            offset = 0
            for part_number, segment in enumerate(manifest):
                container, obj = segment['name'].split('/', 2)[1:]
                segment_meta = self._swift_client.get_object_metadata(
                    self.account, container, obj, req_headers)
                length = int(segment_meta['content-length'])
                resp = s3_client.upload_part_copy(
                    Bucket=self.aws_bucket,
                    CopySource={'Bucket': self.aws_bucket, 'Key': s3_key},
                    CopySourceRange='bytes=%d-%d' % (offset,
                                                     offset + length - 1),
                    Key=s3_key,
                    PartNumber=part_number + 1,
                    UploadId=multipart_resp['UploadId'])
                s3_etag = resp['CopyPartResult']['ETag']
                if not self.check_etag(segment['hash'], s3_etag):
                    raise RuntimeError('Part %d ETag mismatch (%s): %s %s' % (
                                       part_number + 1,
                                       self.account + segment['name'],
                                       segment['hash'], resp['ETag']))
                offset += length

            s3_client.complete_multipart_upload(
                Bucket=self.aws_bucket,
                Key=s3_key,
                MultipartUpload={'Parts': [
                    {'PartNumber': number + 1,
                     'ETag': segment['hash']}
                    for number, segment in enumerate(manifest)]
                },
                UploadId=multipart_resp['UploadId'])

    def update_metadata(self, swift_meta, s3_key):
        self.logger.debug('Updating metadata for %s to %r' % (
            s3_key, convert_to_s3_headers(swift_meta)))
        with self.client_pool.get_client() as boto_client:
            s3_client = boto_client.client
            if not check_slo(swift_meta):
                s3_client.copy_object(
                    CopySource={'Bucket': self.aws_bucket,
                                'Key': s3_key},
                    MetadataDirective='REPLACE',
                    Metadata=convert_to_s3_headers(swift_meta),
                    Bucket=self.aws_bucket,
                    Key=s3_key)

    def _full_name(self, key):
        return u'%s/%s/%s' % (self.account, self.container,
                              key.decode('utf-8'))

    @staticmethod
    def check_etag(swift_etag, s3_etag):
        # S3 ETags are enclosed in ""
        return s3_etag == '"%s"' % swift_etag

    @staticmethod
    def in_glacier(s3_meta):
        if 'StorageClass' in s3_meta and s3_meta['StorageClass'] == 'GLACIER':
            return True
        return False
