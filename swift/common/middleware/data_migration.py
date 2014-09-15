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

from swift.common.utils import get_logger, config_true_value
from swift.common.utils import register_swift_info
from swift.common.swob import Request, HTTPBadRequest, HTTPNotFound
from swift.common.swob import HTTPPreconditionFailed
from swift.proxy.controllers.base import get_container_info
from httplib import NOT_FOUND, CREATED, ACCEPTED, OK
from swift.common.wsgi import WSGIContext
from swift.common.data_migration_common import DataMigrationDriverError
"""
Data migration middleware is used to perform data migration
from another storage cloud or file system into Swift.

Our suggested mechanism provides two main functions:
1. Per container setup. In this step an existing ( or a new )
local Swift container will be "linked" with a
remote container ( or folder ) in another cloud that need not be Swift.
2. Object migration on demand. Any object that will be accessed through
the local Swift container and not yet migrated, will be migrated
immediately and stored in Swift.

Our model comes with variety of storage drivers that
may be used to access remote storage to migrate data.
1. (Default) Migration from file system
2. (Optional) Migration from another Swift, based on python swiftclient.
3. (Optional) User defined custom driver.

Data migration middleware run on Proxy with the configuration as follows:
*************************************************************************
pipeline = catch_errors gatekeeper proxy-logging data-migration proxy-server

[filter:data-migration]
use = egg:swift#data_migration

# List of supported drivers. This is a dynamic list.
supported_drivers = fsystem,swift
# For each driver XXX there should be defined
# driver_XXX_keys ( input parameters for the driver. This is the list of
# required metadata headers to be set on container during data migration
# setup process )
# driver_XXX_module ( the class that implements driver's logic. This class
# will be loaded dynamically during middleware initialization )

# File System driver
driver_fsystem_module =
swift.common.data_migrator_drivers:FileSystemAccessDriver

# Swift driver based on python-swiftclient (Optional)
driver_swift_keys = token-url,user,key
driver_swift_module =
swift.common.data_migrator_drivers:SwiftAccessDriver
*********************************************************

Data Migration Activation
----------------------------
1. Setup phase. To activate data migration there is a need
to "link" an existing Swift container (preferably an empty one) with
container located on another storage. This information is stored as part
of Swift container's metadata and we defined specific metadata keys that
are reserved for this operation. There is also support for
additional custom metadata keys that certain drivers may require.
+---------------------------------------------+-------------------------------+
|Metadata                                     | Use                           |
+=============================================+===============================+
+---------------------------------------------+-------------------------------+
| X-Container-Migration-Active                | Mandatory. Enable of disable  |
|                                             | data migration.               |
|                                             | Values: True, False           |
+---------------------------------------------+-------------------------------+
| X-Container-Migration-Provider              | Mandatory. Old storage type,  |
|                                             | value from supported drivers  |
+---------------------------------------------+-------------------------------+
| X-Container-Migration-Source                | Mandatory. Old container or   |
|                                             | folder that contains objects  |
|                                             | that need to to be migrated   |
+---------------------------------------------+-------------------------------+
| X-Container-Migration-*                     | Optional. Additional metadata |
|                                             | that certain drivers may need |
+---------------------------------------------+-------------------------------+

2. Operational phase - Data Migration on-demand.
After completion of the setup, user may access local Swift container defined in
(1) and request an object via ( GET / HEAD ) even if the object is not yet
exists in Swift. In this case, data migration layer will read an object from
the old cloud or file system, store it locally in Swift and return response
to the user.

3. Subsequent calls to the already migrated object will no longer access old
storage.

This middleware uses system metadata to preserve all migration related metadata

For more details how to setup data migration - see migrator drivers module.
"""


class DataMigrationError(Exception):

    def __init__(self, msg):
        Exception.__init__(self, msg)


class DataMigrationContext(WSGIContext):
    '''
    WSGIContext() has to be used for internal call.
    Otherwise there is no any guarantee that the response will be correct.
    WSGIContext will ensure that all other middleware has had a chance to
    process and set the response code and headers.
    '''
    def __init__(self, wsgi_app):
        WSGIContext.__init__(self, wsgi_app)

    def handle_original_call(self, env, start_response):
        app_resp = self._app_call(env)
        if self._response_headers is None:
            self._response_headers = []
        migration_headers = []
        for key, val in self._response_headers:
            if key in ('X-Container-Sysmeta-Migration-Provider',
                       'X-Container-Sysmeta-Migration-Source',
                       'X-Container-Sysmeta-Migration-Active'):
                migration_headers.append((key.replace('Sysmeta-', ''), val))
        self._response_headers.extend(migration_headers)
        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)
        env['swift.original_status'] = self._get_status_int()
        return app_resp


class DataMigrationMiddleware(object):

    def __init__(self, app, conf, migration_conf):
        self.app = app
        self.logger = get_logger(conf, log_route='data_migration')
        self.migration_conf = migration_conf

    def __call__(self, env, start_response):
        """
        1. Validates the migration setup by inspecting container's metadata.
        2. Activates data migration logic when the following conditions
        are satisfied
         a. GET / HEAD on local object returned '404 Not Found'
         b. Local container to whom object belongs has active data migration

        In case data migration failed for any reason, returns original Swift's
        response '404 Not Found'.
        """
        req = Request(env)
        try:
            (version, account, container, obj) = req.split_path(3, 4, True)
        except ValueError:
            return self.app(env, start_response)
        if not obj and container and account:
            if req.method in ['PUT', 'POST']:
                migration_headers = ['X-Container-Migration-Provider',
                                     'X-Container-Migration-Source',
                                     'X-Container-Migration-Active']
                prov = req.headers.get('X-Container-Migration-Provider')
                mig_source = req.headers.get('X-Container-Migration-Source')
                if (not mig_source) and (prov is not None):
                    return HTTPPreconditionFailed('Migration source is ' +
                                                  'missing')(env,
                                                             start_response)
                elif (not prov) and (mig_source is not None):
                    return HTTPPreconditionFailed('Migration provider is ' +
                                                  'missing')(env,
                                                             start_response)
                elif (prov is not None) and (mig_source is not None):
                    if prov not in self.migration_conf:
                        return HTTPBadRequest(body='Invalid provide' +
                                              'r')(env, start_response)
                    if self.migration_conf[prov]['driver_loaded'] is False:
                        return HTTPBadRequest(body='Invalid access ' +
                                              'driver')(env, start_response)
                    for key in self.migration_conf[prov]["keys"]:
                        if (req.headers.get('X-Container-Migration-' +
                                            key.title()) is None):
                            return HTTPBadRequest(body='Missing required ' +
                                                  'header: X-Container-' +
                                                  'Migration-' +
                                                  key.title())(env,
                                                               start_response)
                        migration_headers.append('X-Container-Migration-' +
                                                 key.title())
                    for key, val in \
                        self.migration_conf[prov]['additional_' +
                                                  'params'].iteritems():
                        if val is None or val.strip() == '':
                            return HTTPBadRequest(body='Missing value for ' +
                                                  key)(env, start_response)

                for key, val in req.headers.iteritems():
                    if key in migration_headers:
                        req.headers[key.replace('X-Container-',
                                                'X-Container-Sysmeta-')] = val

        original_env = req.environ.copy()
        ctx = DataMigrationContext(self.app)
        original_resp = ctx.handle_original_call(env, start_response)

        original_resp_status = env['swift.original_status']
        del env['swift.original_status']
        if original_resp_status == NOT_FOUND:
            if 'X-Container-Migration-Provider' in req.headers.keys():
                return original_resp
            if obj and container and account and req.method in ['GET', 'HEAD']:
                container_info = get_container_info(req.environ, self.app,
                                                    swift_source='DM')
                container_md = container_info.get('sysmeta', {})
                if 'migration-active' in container_md:
                    if config_true_value(container_md['migration-active']):
                        try:
                            return self.GETorHEAD_miss(original_env, obj,
                                                       container_md,
                                                       start_response)
                        except DataMigrationError as e:
                            self.logger.error(e)
                            rh = {'X-Migration-Status': str(e)}
                            return \
                                HTTPNotFound(req=req,
                                             headers=rh)(env, start_response)
                        except Exception as e:
                            self.logger.error(e)
                            rh = {'X-Migration-Status': str(e)}
                            return \
                                HTTPBadRequest(req=req,
                                               headers=rh)(env, start_response)

        return original_resp

    def GETorHEAD_miss(self, original_env, obj, container_md, start_response):
        """
        Data migration logic for GET or HEAD requests. Identifies correct
        access driver and then reads an object from another storage. Received
        object will be written in local Swift and returned back to the user.

        :param original_env: environ of the application
        :param obj: object name
        :param container_md: metadata of the local container
        :param start_response: start_response of the WSGI
        :returns Response with migrated object.
        :raises DataMigrationError when operation fails
        """
        orig_env = dict(original_env)
        access_resolver = self.remote_driver_resolver(container_md,
                                                      self.migration_conf)

        try:
            status = access_resolver.migrate_object(obj, original_env,
                                                    self.app)
        except DataMigrationDriverError as e:
            raise DataMigrationError(str(e))

        access_resolver.finalize()

        if (status in [OK, CREATED, ACCEPTED]):
            ctx = DataMigrationContext(self.app)
            return ctx.handle_original_call(orig_env, start_response)
        else:
            raise DataMigrationError('Failed to create local object.' +
                                     'Status {0}'.format(status))

    def remote_driver_resolver(self, container_md, migration_conf):
        """
        Using Swift's container metadata to identify a correct access
        driver to the old storage.
        :param container_md: metadata of the Swift's container
        :param migration_conf: middleware configuration data
        :returns a handler pointing to the driver class.
        :raises DataMigrationError if operation failed.
        """
        mprovider = container_md.get('migration-provider', '').lower()
        data_source = container_md.get('migration-source', '').lower()
        if mprovider not in migration_conf:
            raise DataMigrationError('Migration provider is missing')
        the_class = migration_conf[mprovider]['the_class']
        params = dict()
        if migration_conf[mprovider]['driver_loaded'] is True:
            for key in migration_conf[mprovider]['keys']:
                params[key] = container_md.get('migration-' + key, '')
            for key, val in migration_conf[mprovider]['additional_' +
                                                      'params'].iteritems():
                params[key] = val
            try:
                return the_class(data_source, params)
            except DataMigrationDriverError as e:
                raise DataMigrationError(str(e))

        raise DataMigrationError('Failed to retrieve remote driver')


def filter_factory(global_conf, **local_conf):
    """
    Parse the configuration. Parsing the list of supported drivers.
    For each driver generates a map with keys and class implementation.
    """
    conf = global_conf.copy()
    conf.update(local_conf)
    migration_conf = {}
    swift_info = {}
    supported_drivers = [driver.strip()
                         for driver in conf.get('supported_drivers',
                                                '').split(',')
                         if driver.strip()]
    for dr in supported_drivers:
        reserved = ['driver_' + dr + '_keys', 'driver_' + dr + '_module']
        keys = [key.strip()
                for key in conf.get('driver_' + dr + '_keys',
                                    '').split(',')
                if key.strip()]
        module_name = conf.get('driver_' + dr + '_module', '')
        mo = module_name[:module_name.rfind(':')]
        cl = module_name[module_name.rfind(':') + 1:]
        module = __import__(mo, fromlist=[cl])
        the_class = getattr(module, cl)
        add_params = {}
        for key, val in conf.iteritems():
            if key not in reserved and key.startswith('driver_' + dr):
                add_params[key] = val
        migration_conf[dr] = {'keys': keys, 'the_class': the_class,
                              'driver_loaded': the_class.driver_loaded,
                              'additional_params': add_params}
        if (the_class.driver_loaded):
            swift_info[dr] = 'enabled'
    register_swift_info('data_migration', False, **swift_info)

    def data_migration_filter(app):
        return DataMigrationMiddleware(app, conf, migration_conf)
    return data_migration_filter
