# Copyright 2014 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import os
import stat
import mimetypes
from xattr import xattr
from swift.common.request_helpers import is_user_meta
from swift.common.data_migration_common import DataMigrationDriver
from swift.common.data_migration_common import DataMigrationDriverError

SWIFTCLIENT_IMPORTED = False
try:
    from swiftclient import client, http_connection, quote
    SWIFTCLIENT_IMPORTED = True
except ImportError:
    pass


class FileSystemAccessDriver(DataMigrationDriver):
    """
    A driver to migrate objects from disk.
    It is mandatory to set `driver_fsystem_parent_path` with non
    empty value in the proxy-server configration prior usage of this driver.
    This will be used a root path for all migration process.

    To use this driver the following container metadata required:

    X-Container-Migration-Active: True
    X-Container-Migration-Provider: fsystem
    X-Container-Migration-Source: folder that contains objects. Will be
    treated as a sub-folder of `driver_fsystem_parent_path`

    e.g. to migrate objects from folder /home/vacation/images
    driver_fsystem_parent_path = /home/
    X-Container-Migration-Source: vacation/images
    """
    object_to_migrate = None
    driver_loaded = True

    def __init__(self, data_source, params):
        """
        :param data_source:  a folder that contains objects.
        :param params: additional parameters to the driver

        """
        self.root_path = params.get('driver_fsystem_parent_path')

        if not self.is_valid_path(self.root_path):
            raise DataMigrationDriverError('driver_fsystem_parent_path: ' +
                                           '{0} '.format((self.root_path)) +
                                           ' is invalid')

        if not self.is_valid_path(data_source):
            raise DataMigrationDriverError('Migration source ' +
                                           '{0} '.format((data_source)) +
                                           'is invalid')
        if data_source.startswith(os.path.sep):
            data_source = data_source[1:]

        self.data_source = os.path.join(os.path.sep, self.root_path,
                                        data_source)

    def is_valid_path(self, path):
        if path is None:
            raise Exception('driver_fsystem_parent_path ' +
                            'parameter should be configured' +
                            ' in proxy-server.conf')
        if path.strip() == '' or path.find('..') >= 0:
            return False
        return True

    def get_object(self, object_name):
        """
        Read an object from filesystem

        :param object_name: name of the object
        :returns (metadata, object size, data stream, content type, timestamp)
        """
        file_path = os.path.join(os.path.sep, self.data_source, object_name)
        try:
            statinfo = os.stat(file_path)
        except Exception:
            raise DataMigrationDriverError('Failed to access object ' +
                                           'in file system')
        if not stat.S_ISDIR(statinfo.st_mode):
            md = dict()
            md['uid'] = statinfo.st_uid
            md['gid'] = statinfo.st_gid
            last_modified = statinfo.st_mtime
            content_type, encoding = mimetypes.guess_type(file_path)
            self.object_to_migrate = open(file_path, "r")
            try:
                extended_md = xattr(self.object_to_migrate)
                for key, val in extended_md.iteritems():
                    md[key] = val
            except IOError:
                pass
            return md, statinfo.st_size, self.object_to_migrate, \
                content_type, last_modified
        return {}, None, None, None, None

    def finalize(self):
        self.object_to_migrate.close()


class SwiftAccessDriver(DataMigrationDriver):
    """
    Driver to migrate data from Swift based on swift client.

    This is an optional implementation and one need to install
    python-swiftclient in advance.

    Mandatory metadata during setup process

    X-Container-Migration-Active: True
    X-Container-Migration-Provider: swift
    X-Container-Migration-Token-Url: token URL,
    e.g. http://127.0.0.1:8080/auth/v1.0
    X-Container-Migration-Source: container that contains objects
    X-Container-Migration-User: credentials to access
    source container, e.g. test:tester
    X-Container-Migration-Key: key, e.g testing
    """
    driver_loaded = SWIFTCLIENT_IMPORTED

    def __init__(self, data_source, params):
        """
        :param data_source:  a container that contains objects.
        :param params: additional parameters to the driver
        """
        self.data_source = data_source
        self.token_url = params.get('token-url', '')
        self.user = params.get('user', '')
        self.key = params.get('key', '')

    def get_object(self, object_name):
        """
        Read an object from the remote cloud

        :param object_name: name of the object
        :returns (metadata, object size, data stream, content type, timestamp)
        """
        if SWIFTCLIENT_IMPORTED:
            try:
                storage_url, token = client.get_auth(self.token_url,
                                                     self.user, self.key)
            except Exception as e:
                raise DataMigrationDriverError(str(e))
            parsed, conn = http_connection(storage_url)
            path = '%s/%s/%s' % (parsed.path, quote(self.data_source),
                                 quote(object_name))
            try:
                conn.request('GET', path, '', {'X-Auth-Token': token,
                                               'X-Container-Migration-' +
                                               'Provider': 'swift'})
                resp = conn.getresponse()
            except Exception:
                raise DataMigrationDriverError('Connection failed to ' +
                                               '%s' % storage_url)
            if resp.status < 200 or resp.status >= 300:
                body = resp.read()
                raise DataMigrationDriverError('Object GET failed: %s' % body)

            resp_headers = dict()
            metadata = dict()
            for key, value in resp.getheaders():
                key = key.lower()
                resp_headers[key] = value
                if is_user_meta('object', key):
                    metadata[key] = value
            return metadata, resp_headers.get('content-length'),\
                resp, resp_headers.get('content-type'), \
                resp_headers.get('x-timestamp')
        return {}, None, None, None, None
