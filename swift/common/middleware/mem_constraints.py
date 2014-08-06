# Copyright (c) 2012-2014 Red Hat, Inc.
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

"""
``mem_constraints`` is a middleware which will check storage policies
specific constraints on PUT requests.

The ``constraints`` middleware should be added to the pipeline in your
``/etc/swift/proxy-server.conf`` file, and a mapping of storage policies and
constraints classes be listed under the constraints filter section.
For example::

    [pipeline:main]
    pipeline = catch_errors constraints cache proxy-server

    [filter:constraints]
    use = egg:swift#mem_constraints
    policies=policy_2
"""

from urllib import unquote
from swift.common.utils import get_logger
from swift.common.swob import Request
from swift.proxy.controllers.base import get_container_info
from swift.common.constraints import check_object_creation
from swift.common.swob import HTTPBadRequest, HTTPRequestEntityTooLarge


class CheckConstraintsMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route='constraints')
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.policies = conf.get('policies', '')
        self.max_object_name_length = conf.get('max_object_name_length', 128)
        self.max_file_size = conf.get('max_file_size', 1048576)

    def __call__(self, env, start_response):
        request = Request(env)

        if request.method == 'PUT':
            try:
                version, account, container, obj = \
                    request.split_path(1, 4, True)
            except ValueError:
                return self.app(env, start_response)

            if obj is not None:
                obj = unquote(obj)
            else:
                return self.app(env, start_response)

            container_info = get_container_info(
                env, self.app, swift_source='LE')
            policy_idx = 'policy_%s' % container_info['storage_policy']
            if policy_idx in self.policies:
                env['swift.constraints'] = self.check_object_creation

        return self.app(env, start_response)

    def check_object_creation(self, req, object_name):
        if len(object_name) > self.max_object_name_length:
            return HTTPBadRequest(
                body='Object name length of %d longer than %d' %
                (len(object_name), self.max_object_name_length),
                request=req, content_type='text/plain')

        if req.content_length and req.content_length > self.max_file_size:
            return HTTPRequestEntityTooLarge(
                body='Your request is too large.', request=req,
                content_type='text/plain')

        return check_object_creation(req, object_name)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def check_constraints_filter(app):
        return CheckConstraintsMiddleware(app, conf)

    return check_constraints_filter
