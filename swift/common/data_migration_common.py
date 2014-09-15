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

from swift.common.swob import Request
import time


class DataMigrationDriver(object):
    """
    A common base class for all drivers.
    Method get_object should be implemented by each
    derived class
    """

    def get_object(self, object_name):
        """
        :param object_name: the object name
        """
        pass

    def finalize(self):
        pass

    def migrate_object(self, obj, original_env, app):
        """
        :param obj: object name
        :param original_env: original environ of the application
        """
        metadata, read_length, body_stream, \
            content_type, timestamp = self.get_object(obj)

        if body_stream is None:
            raise DataMigrationDriverError(
                'Failed to retrieve object for migration')

        sys_metadata = dict()
        sys_metadata['Migration-Import-Time'] = str(time.time())
        sys_metadata['Migration-Import-Owner'] = 'On-Demand'

        status = self.upload_object(original_env, body_stream, read_length,
                                    metadata, sys_metadata, content_type,
                                    timestamp, app)
        return status

    def upload_object(self, env, data, length, metadata, sys_metadata,
                      content_type, timestamp, app):
        """
        Swift internal call to upload an object to Swift.

        :param env: environ of the application
        :param data: object's data as an iterator
        :param length: size of an object
        :param metadata: metadata of an object
        :param sys_metadata: system metadata of an object
        :param content_type: content type of an object
        :param timestamp: timestamp of an object in the old storage
        :returns HTTP code result of an upload operation
        """
        new_env = dict(env)
        new_env['REQUEST_METHOD'] = 'PUT'
        new_env['wsgi.input'] = data
        new_env['CONTENT_LENGTH'] = length
        new_env['CONTENT_TYPE'] = content_type
        new_env['swift.source'] = 'DM'
        create_obj_req = Request.blank(new_env['PATH_INFO'], new_env)
        create_obj_req.headers['X-Timestamp'] = min(time.time(),
                                                    float(timestamp))
        for key in metadata.keys():
            if key.lower().startswith('x-object-meta-'):
                create_obj_req.headers[key] = metadata[key]
            else:
                create_obj_req.headers['X-Object-Meta-' +
                                       key] = metadata[key]
        for key in sys_metadata.keys():
            create_obj_req.headers['X-Object-Sysmeta-' +
                                   key] = sys_metadata[key]

        resp = create_obj_req.get_response(app)
        return resp.status_int


class DataMigrationDriverError(Exception):

    def __init__(self, msg):
        Exception.__init__(self, msg)
