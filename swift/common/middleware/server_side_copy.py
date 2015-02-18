# Copyright (c) 2015 OpenStack Foundation
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
Server side copy is a feature that enables users/clients to COPY objects
between storage servers without the need for client to download and then
re-upload objects, thus eliminating additional bandwidth consumption and
also saving time. This is analogous to renaming/moving an object which
in Swift is a (COPY + DELETE) operation.

The server side copy middleware is auto-inserted after dlo, slo and
versioned middleware. This is done so, because:
* versioned middleware invokes COPY requests internally
* DLO and SLO middlewares install copy hooks that needs to be invoked from
  server side copy middleware

There is no configurable option provided to turn off server side copy.

--------
Metadata
--------
* All metadata of source object is preserved during object copy.
* One can also provide additional metadata during PUT/COPY request. This will
  over-write any existing conflicting keys.
* Server side copy can also be used to change content-type of an existing
  object.

-----------
Object Copy
-----------
* The destination container must exist before requesting copy of the object.
* When several replicas exist, the system copies from the most recent replica.
  That is, the copy operation behaves as though the X-Newest header is in the
  request.
* The request to copy an object should have no body i.e content-length of the
  request must be zero.

There are two ways in which an object can be copied:

1. Send a PUT request to the new object (destination/target) with an additional
   header named 'X-Copy-From' specifying the source object
   (in 'container/object' format)
   Example:
   curl -i XPUT http://<storage_url>/container1/destination_object
    -H 'X-Auth-Token: <token>'
    -H 'X-Copy-From: /container2/source_object'
    -H 'Content-Length: 0'

2. Send a COPY request with an existing object in URL with an additional header
   named 'Destination' specifying the destination/target object
   (in '/container/object' format)
   Example:
   curl -i COPY http://<storage_url>/container2/source_object
    -H 'X-Auth-Token: <token>'
    -H 'Destination: /container1/destination_object'
    -H 'Content-Length: 0'

-------------------------
Cross Account Object Copy
-------------------------
Objects can also be copied from one account to another account if the user
has the necessary permissions to do so i.e permission to read from container
in source account and permission to write to container in destination account.

Similar to examples mentioned above, there are two ways to copy objects across
accounts:

1. Like the example above, send PUT request to copy object but with an
   additional header named 'X-Copy-From-Account' specifying the source account
   Example:
   curl -i XPUT http://<host>:<port>/v1/AUTH_test1/container/destination_object
    -H 'X-Auth-Token: <token>'
    -H 'X-Copy-From: /container/source_object'
    -H 'X-Copy-From-Account: AUTH_test2'
    -H 'Content-Length: 0'

2. Like the previous example, send a COPY request but with an additional header
   named 'Destination-Account' specifying the name of destination account.
   Example:
   curl -i COPY http://<host>:<port>/v1/AUTH_test2/container/source_object
    -H 'X-Auth-Token: <token>'
    -H 'Destination: /container/destination_object'
    -H 'Destination-Account: AUTH_test1'
    -H 'Content-Length: 0'

-------------------
Object Post As Copy
-------------------
Historically, this has been a feature (and a configurable option with default
set to yes) in proxy server app. This has been moved to server side copy
middleware.

When object_post_as_copy is set to True, an incoming POST request is morphed
into a PUT/COPY request where source and destination objects are same. Here
a new copy of the object is created which ensures that any new metadata added
or modified gets updated in container server. This enables features like
container sync to sync POSTs.

When object_post_as_copy is set to False, only the metadata changes are stored
anew and the original data file is kept in place. This makes for quicker
posts; but since the container metadata isn't updated in this mode, features
like container sync won't be able to sync POSTs.
"""

import os
from urllib import quote
from ConfigParser import ConfigParser, NoSectionError, NoOptionError
from swift.common.utils import get_logger, config_true_value, read_conf_dir
from swift.common.swob import Request, HTTPPreconditionFailed, HTTPBadRequest
from swift.common.constraints import (check_account_format,
                                      check_destination_header,
                                      check_copy_from_header)
from swift.common.copy_helper import CopyHelper, ServerSideCopyWebContext


class ServerSideCopyMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route="server_side_copy")
        # Read the old object_post_as_copy option from Proxy app just in case
        # someone has set it to false (non default). This wouldn't cause
        # problems during upgrade.
        self._load_object_post_as_copy_conf(conf)
        self.object_post_as_copy = \
            config_true_value(conf.get('object_post_as_copy', 'true'))

    def _load_object_post_as_copy_conf(self, conf):
        if ('object_post_as_copy' in conf or '__file__' not in conf):
            # Option is explicitly set in middleware conf. In that case,
            # we assume operator knows what he's doing.
            # This takes preference over the one set in proxy app
            return

        cp = ConfigParser()
        if os.path.isdir(conf['__file__']):
            read_conf_dir(cp, conf['__file__'])
        else:
            cp.read(conf['__file__'])

        try:
            pipe = cp.get("pipeline:main", "pipeline")
        except (NoSectionError, NoOptionError):
            return

        proxy_name = pipe.rsplit(None, 1)[-1]
        proxy_section = "app:" + proxy_name

        try:
            conf['object_post_as_copy'] = cp.get(proxy_section,
                                                 'object_post_as_copy')
        except (NoSectionError, NoOptionError):
            pass

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            (version, account, container, obj) = req.split_path(4, 4, True)
        except ValueError:
            # If obj component is not present in req, do not proceed further.
            return self.app(env, start_response)

        self.account_name = account
        self.container_name = container
        self.object_name = obj

        # Save off original request method (COPY/POST) in case it gets mutated
        # into PUT during handling. This way logging can display the method
        # the client actually sent.
        req.environ['swift.orig_req_method'] = req.method

        if req.method == 'PUT' and req.headers.get('X-Copy-From'):
            return self.handle_PUT(req, start_response)
        elif req.method == 'COPY':
            return self.handle_COPY(req, start_response)
        elif req.method == 'POST' and self.object_post_as_copy:
            return self.handle_object_post_as_copy(req, start_response)
        elif req.method == 'OPTIONS':
            # Does not interfere with OPTIONS response from (account,container)
            # servers and /info response.
            return self.handle_OPTIONS(req, start_response)

        return self.app(env, start_response)

    def handle_object_post_as_copy(self, req, start_response):
        req.method = 'PUT'
        req.path_info = '/v1/%s/%s/%s' % (
            self.account_name, self.container_name, self.object_name)
        req.headers['Content-Length'] = 0
        req.headers['X-Copy-From'] = quote('/%s/%s' % (self.container_name,
                                           self.object_name))
        req.environ['swift.post_as_copy'] = True
        return self.handle_PUT(req, start_response)

    def handle_COPY(self, req, start_response):
        if not req.headers.get('Destination'):
            return HTTPPreconditionFailed(request=req,
                                          body='Destination header required'
                                          )(req.environ, start_response)
        dest_account = self.account_name
        if 'Destination-Account' in req.headers:
            dest_account = req.headers.get('Destination-Account')
            dest_account = check_account_format(req, dest_account)
            req.headers['X-Copy-From-Account'] = self.account_name
            self.account_name = dest_account
            del req.headers['Destination-Account']
        dest_container, dest_object = check_destination_header(req)
        source = '/%s/%s' % (self.container_name, self.object_name)
        self.container_name = dest_container
        self.object_name = dest_object
        # re-write the existing request as a PUT instead of creating a new one
        req.method = 'PUT'
        # As this the path info is updated with destination container,
        # the proxy server app will use the right object controller
        # implementation corresponding to the container's policy type.
        req.path_info = '/v1/%s/%s/%s' % \
                        (dest_account, dest_container, dest_object)
        req.headers['Content-Length'] = 0
        req.headers['X-Copy-From'] = quote(source)
        del req.headers['Destination']
        return self.handle_PUT(req, start_response)


    def handle_PUT(self, req, start_response):

        if int(req.headers['Content-Length']) != 0:
            return HTTPBadRequest(body='Copy requests require a zero byte '
                                  'body', request=req,
                                  content_type='text/plain')(req.environ,
                                                             start_response)

        if req.environ.get('swift.orig_req_method', req.method) != 'POST':
            req.environ.setdefault('swift.log_info', []).append(
                'x-copy-from:%s' % req.headers.get('X-Copy-From'))

        # Form the path of source object to be fetched
        ver, acct, _rest = req.split_path(2, 3, True)
        src_account_name = req.headers.get('X-Copy-From-Account', None)
        if src_account_name:
            src_account_name = check_account_format(req, src_account_name)
        else:
            src_account_name = acct
        src_container_name, src_obj_name = check_copy_from_header(req)
        source_path = '/%s/%s/%s/%s' % (ver, src_account_name,
                                        src_container_name, src_obj_name)
        copy_helper = CopyHelper(self.app, self.logger)
        return copy_helper.copy(source_path, req, start_response)

    def handle_OPTIONS(self, req, start_response):
        return ServerSideCopyWebContext(self.app, self.logger).\
            handle_OPTIONS_request(req, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def server_side_copy_filter(app):
        return ServerSideCopyMiddleware(app, conf)

    return server_side_copy_filter
