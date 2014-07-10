# Copyright 2014 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from swift.common.middleware.data_migration import DataMigrationError
from swift.common.data_migrator_drivers import SwiftAccessDriver, \
    FileSystemAccessDriver
from swift.common.utils import split_path
from swift.common.swob import Request, Response
from swift.common.middleware import data_migration
import tempfile
import os
import imp

local_files = ['obj1.dat']
global_conf = {'log_name': 'proxy-server',
               'eventlet_debug': 'true',
               'bind_port': '8080',
               '__file__': '/etc/swift/proxy-server.conf',
               'here': '/etc/swift',
               'log_facility': 'LOG_LOCAL1',
               'user': 'swift',
               'log_level': 'DEBUG',
               'workers': '1',
               'driver_fsystem_parent_path': '/tmp/'}
local_conf = {'driver_swift_keys': 'token-url,user,key',
              'supported_drivers': 'fsystem,swift',
              'driver_swift_module': 'swift.common.data_migrator_' +
              'drivers:SwiftAccessDriver',
              'driver_fsystem_module': 'swift.common.data_migrator_' +
              'drivers:FileSystemAccessDriver'}


class FakeApp(object):
    GET_counter = 0

    def __call__(self, env, start_response):
        req = Request(env)
        (version, account, container, obj) = split_path(req.path, 1, 4, True)
        resp = None
        if (req.method in ['GET', 'HEAD']):
            if (self.GET_counter == 0):
                resp = Response(status='404 Not Found',
                                body='', request=req)
            elif (self.GET_counter == 1):
                resp = Response(status='200 OK',
                                body='', request=req)
            self.GET_counter = int(self.GET_counter) + 1
            return resp(env, start_response)
        elif (req.method == 'PUT'):
            generated_status = '201 Created'
            if (account and container and obj):
                res = verify_object(req, container, obj)
                generated_status = '400 Bad Request'
                if (res):
                    generated_status = '200 OK'
            else:
                generated_status = '201 Created'
            return Response(status=generated_status,
                            body='',
                            request=req)(env, start_response)
        return Response(status='400 Bad Request',
                        body='',
                        request=req)(env, start_response)


def verify_object(req, container_name, object_name):
    newpath = os.path.join(tempfile.gettempdir(), 'migsource')
    file_path = os.path.join(newpath, object_name)
    f = open(file_path, 'r+')
    original_data = f.read()
    res = True
    if original_data != req.body:
        res = False
    fsize = os.path.getsize(file_path)
    content_length = req.headers['Content-Length']
    if (content_length != fsize):
        res = False
    f.close()
    return res


def mock_get_container_info(env, app, swift_source='SW'):
    container = env['PATH_INFO'].rstrip('/').split('/')[3]
    fsystem_migration_info = {'migration-active': 'True',
                              'migration-provider': 'fsystem',
                              'migration-source': ''}
    swiftclient_info = {'X-Container-Migration-Active': 'True',
                        'X-Container-Migration-Provider': 'swift',
                        'X-Container-Migration-' +
                        'Token-Url': 'http://127.0.0.1:8080/auth/v1.0',
                        'X-Container-Migration-Source': 'old_container',
                        'X-Container-Migration-User': 'test:tester',
                        'X-Container-Migration-Key': 'testing'}

    if (container == 'cfs1'):
        container_info = {'meta': fsystem_migration_info.copy()}
        container_info['meta']['migration-source'] = \
            os.path.join(tempfile.gettempdir(), 'migsource')
        container_info.setdefault('status', 200)
        return container_info
    elif (container == 'cfswrongpath'):
        container_info = {'meta': fsystem_migration_info.copy()}
        container_info['meta']['migration-source'] = '/wrongpath'
        container_info.setdefault('status', 200)
        return container_info
    elif (container == 'cswift1'):
        container_info = {'meta': swiftclient_info.copy()}
        return container_info


class TestDataMigrationFileSystem(unittest.TestCase):
    fsystem_migration = {'X-Container-Migration-Active': 'True',
                         'X-Container-Migration-Provider': 'fsystem',
                         'X-Container-Migration-Source': ''}

    def setUp(self):
        self.app = data_migration.filter_factory(global_conf,
                                                 **local_conf)(FakeApp())
        self._orig_get_container_info = data_migration.get_container_info
        data_migration.get_container_info = mock_get_container_info

    def tearDown(self):
        data_migration.get_container_info = self._orig_get_container_info

    def test_migration_setup_fsystem_ok(self):
        fsystem_setup = self.fsystem_migration.copy()
        fsystem_setup['X-Container-Migration-Source'] = 'a/b/c/d/e'
        req = Request.blank('/v1/a/cfs1',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=fsystem_setup)
        res = req.get_response(self.app)
        self.assertEquals(res.status_int, 201)

    def test_migration_setup_fsystem_wrong_provider(self):
        fsystem_setup = self.fsystem_migration.copy()
        fsystem_setup['X-Container-Migration-Source'] = 'a/b/c/d/e'
        fsystem_setup['X-Container-Migration-Provider'] = 'fiction'
        req = Request.blank('/v1/a/cfs1',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=fsystem_setup)
        res = req.get_response(self.app)
        self.assertEquals(res.status_int, 400)

    def test_migration_setup_fsystem_no_provider(self):
        fsystem_setup = self.fsystem_migration.copy()
        fsystem_setup['X-Container-Migration-Source'] = 'a/b/c/d/e'
        del fsystem_setup['X-Container-Migration-Provider']
        req = Request.blank('/v1/a/cfs1',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=fsystem_setup)
        res = req.get_response(self.app)
        self.assertEquals(res.status_int, 412)

    def test_migration_setup_fsystem_missing_param(self):
        fsystem_setup = self.fsystem_migration.copy()
        del fsystem_setup['X-Container-Migration-Source']
        req = Request.blank('/v1/a/cfs1',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=fsystem_setup)
        res = req.get_response(self.app)
        self.assertEquals(res.status_int, 412)

    def test_data_migration_fsystem_GET_wrong_path(self):
        req = Request.blank('/v1/a/cfswrongpath/' + local_files[0],
                            environ={'REQUEST_METHOD': 'GET'})
        res = req.get_response(self.app)
        self.assertEquals(res.status_int, 404)


class TestDataMigrationSwiftClient(unittest.TestCase):
    global_conf = {'log_name': 'proxy-server',
                   'eventlet_debug': 'true',
                   'bind_port': '8080',
                   '__file__': '/etc/swift/proxy-server.conf',
                   'here': '/etc/swift',
                   'log_facility': 'LOG_LOCAL1',
                   'user': 'swift',
                   'log_level': 'DEBUG',
                   'workers': '1'}
    local_conf = {'driver_swift_keys': 'token-url,user,key',
                  'supported_drivers': 'fsystem,swift',
                  'driver_swift_module': 'swift.common.data_' +
                  'migrator_drivers:SwiftAccessDriver',
                  'driver_fsystem_module': 'swift.common.data_' +
                  'migrator_drivers:FileSystemAccessDriver'}
    headers = {'X-Container-Migration-Active': 'True',
               'X-Container-Migration-Provider': 'swift',
               'X-Container-Migration-' +
               'Token-Url': 'http://127.0.0.1:8080/auth/v1.0',
               'X-Container-Migration-Source': 'old_container',
               'X-Container-Migration-User': 'test:tester',
               'X-Container-Migration-Key': 'testing'}

    driver_loaded = False
    try:
        imp.find_module('swiftclient')
        driver_loaded = True
    except ImportError:
        pass

    def setUp(self):
        self.app = data_migration.filter_factory(global_conf,
                                                 **local_conf)(FakeApp())
        self._orig_get_container_info = data_migration.get_container_info
        data_migration.get_container_info = mock_get_container_info

    def tearDown(self):
        data_migration.get_container_info = self._orig_get_container_info

    def test_migration_setup_swiftclient_ok(self):
        md = self.headers.copy()
        md['X-Container-Migration-Source'] = 'oldcontainer'
        req = Request.blank('/v1/a/cswift1',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=md)
        res = req.get_response(self.app)
        if (self.driver_loaded):
            self.assertEquals(res.status_int, 201)
        else:
            self.assertEquals(res.status_int, 503)


class TestDataMigration(unittest.TestCase):
    def setUp(self):
        self.app = data_migration.filter_factory(global_conf,
                                                 **local_conf)(FakeApp())

    def test_remote_driver_resolver_swift(self):
        keys = ['token-url', 'user', 'key']
        add_params = {'driver_fsystem_parent_path': '/tmp/'}
        migration_conf = {'swift': {'the_class': SwiftAccessDriver,
                                    'driver_loaded': True,
                                    'keys': keys,
                                    'additional_params': add_params}}
        container_md = {'migration-provider': 'swift',
                        'migration-source': 'a_source',
                        'migration-token-url': 'http://example.com',
                        'migration-user': 'username',
                        'migration-key': 'secret'}
        driver = self.app.remote_driver_resolver(container_md, migration_conf)
        self.assertTrue(isinstance(driver, SwiftAccessDriver))
        self.assertEqual(driver.data_source, 'a_source')
        self.assertEqual(driver.user, 'username')
        self.assertEqual(driver.key, 'secret')
        self.assertEqual(driver.token_url, 'http://example.com')

    def test_remote_driver_resolver_file_system(self):
        keys = []
        add_params = {'driver_fsystem_parent_path': '/tmp/'}
        migration_conf = {'fsystem': {'the_class': FileSystemAccessDriver,
                                      'driver_loaded': True,
                                      'keys': keys,
                                      'additional_params': add_params}}
        container_md = {'migration-provider': 'fsystem',
                        'migration-source': '/a/b/c/a_source'}
        driver = self.app.remote_driver_resolver(container_md, migration_conf)
        self.assertTrue(isinstance(driver, FileSystemAccessDriver))
        self.assertEqual(driver.data_source, '/tmp/a/b/c/a_source')

    def test_remote_driver_resolver_not_loaded(self):
        keys = []
        add_params = {'driver_fsystem_parent_path': '/tmp/'}
        migration_conf = {'fsystem': {'the_class': FileSystemAccessDriver,
                                      'driver_loaded': False,
                                      'keys': keys,
                                      'additional_params': add_params}}
        container_md = {'migration-provider': 'fsystem'}
        self.assertRaises(DataMigrationError, self.app.remote_driver_resolver,
                          container_md, migration_conf)

    def test_remote_driver_resolver_not_found(self):
        keys = []
        add_params = {'driver_fsystem_parent_path': '/tmp/'}
        migration_conf = {'fsystem': {'the_class': FileSystemAccessDriver,
                                      'driver_loaded': True,
                                      'keys': keys,
                                      'additional_params': add_params}}
        container_md = {'migration-provider': 'bad'}
        self.assertRaises(DataMigrationError, self.app.remote_driver_resolver,
                          container_md, migration_conf)

    def test_driver_fsystem_parent_path_not_found(self):
        keys = []
        add_params = {}
        migration_conf = {'fsystem': {'the_class': FileSystemAccessDriver,
                                      'driver_loaded': True,
                                      'keys': keys,
                                      'additional_params': add_params}}
        container_md = {'migration-provider': 'fsystem'}
        self.assertRaises(Exception, self.app.remote_driver_resolver,
                          container_md, migration_conf)

    def test_driver_fsystem_parent_path_invalid(self):
        keys = []
        add_params = {'driver_fsystem_parent_path': '/../tmp'}
        migration_conf = {'fsystem': {'the_class': FileSystemAccessDriver,
                                      'driver_loaded': True,
                                      'keys': keys,
                                      'additional_params': add_params}}
        container_md = {'migration-provider': 'fsystem'}
        self.assertRaises(DataMigrationError, self.app.remote_driver_resolver,
                          container_md, migration_conf)

    def test_driver_fsystem_parent_path_empty(self):
        keys = []
        add_params = {'driver_fsystem_parent_path': ''}
        migration_conf = {'fsystem': {'the_class': FileSystemAccessDriver,
                                      'driver_loaded': True,
                                      'keys': keys,
                                      'additional_params': add_params}}
        container_md = {'migration-provider': 'fsystem'}
        self.assertRaises(DataMigrationError, self.app.remote_driver_resolver,
                          container_md, migration_conf)

    def test_driver_fsystem_parent_path_none(self):
        keys = []
        add_params = {'driver_fsystem_parent_path': None}
        migration_conf = {'fsystem': {'the_class': FileSystemAccessDriver,
                                      'driver_loaded': True,
                                      'keys': keys,
                                      'additional_params': add_params}}
        container_md = {'migration-provider': 'fsystem'}
        self.assertRaises(Exception, self.app.remote_driver_resolver,
                          container_md, migration_conf)

if __name__ == '__main__':
    unittest.main()
