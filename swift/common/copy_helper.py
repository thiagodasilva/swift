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

from urllib import quote
from StringIO import StringIO
from swift.common.utils import config_true_value, InputProxy
from swift.common.swob import Request, HTTPRequestEntityTooLarge
from swift.common.constraints import MAX_FILE_SIZE
from swift.common.http import HTTP_MULTIPLE_CHOICES
from swift.common.http import HTTP_CREATED, is_success
from swift.common.wsgi import WSGIContext, make_subrequest
from swift.proxy.controllers.obj import copy_headers_into
from swift.common.request_helpers import (copy_header_subset, remove_items,
                                          is_sys_or_user_meta, is_sys_meta)


class ServerSideCopyWebContext(WSGIContext):

    def __init__(self, wsgi_app, logger):
        super(ServerSideCopyWebContext, self).__init__(wsgi_app)
        self.server_side_copy = wsgi_app
        self.logger = logger

    def get_source_resp(self, req):
        sub_req = make_subrequest(
            req.environ, path=req.path_info, method=req.method,
            headers=req.headers, swift_source='SSC')
        return sub_req.get_response(self.server_side_copy.app)

    def send_put_req(self, req, additional_resp_headers, start_response):
        app_resp = self._app_call(req.environ)
        self._adjust_put_response(additional_resp_headers,
                                  req.environ['swift.orig_req_method'])
        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)
        return app_resp

    def _adjust_put_response(self, additional_resp_headers,
                             original_req_method):
        if is_success(self._get_status_int()):
            for header, value in additional_resp_headers.items():
                self._response_headers.append((header, value))
        if original_req_method == 'POST':
            # Older editions returned 202 Accepted on object POSTs, so we'll
            # convert any 201 Created responses to that for compatibility with
            # picky clients.
            if self._get_status_int() == HTTP_CREATED:
                self._response_status = '202 Accepted'

    def handle_OPTIONS_request(self, req, start_response):
        app_resp = self._app_call(req.environ)
        if is_success(self._get_status_int()):
            for i in xrange(len(self._response_headers)):
                header = self._response_headers[i][0]
                value = self._response_headers[i][1]
                if header.lower() == 'allow' and 'COPY' not in value:
                    self._response_headers[i] = ('Allow', value + ', COPY')
                if header.lower() == 'access-control-allow-methods' and \
                        'COPY' not in value:
                    self._response_headers[i] = \
                        ('Access-Control-Allow-Methods', value + ', COPY')
        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)
        return app_resp


class CopyHelper(object):

    def __init__(self, wsgi_app, logger):
        self.wsgi_app = wsgi_app
        self.logger = logger

    def _create_response_headers(self, source_path, source_resp, sink_req):
        resp_headers = dict()
        acct, path = source_path.split('/', 3)[2:4]
        resp_headers['X-Copied-From-Account'] = quote(acct)
        resp_headers['X-Copied-From'] = quote(path)
        if 'last-modified' in source_resp.headers:
                resp_headers['X-Copied-From-Last-Modified'] = \
                    source_resp.headers['last-modified']
        # Existing sys and user meta of source object is added to response
        # headers in addition to the new ones.
        for k, v in sink_req.headers.items():
            if is_sys_or_user_meta('object', k) or k.lower() == 'x-delete-at':
                resp_headers[k] = v
        return resp_headers

    def _get_source_object(self, ssc_ctx, source_path, req):
        source_req = req.copy_get()

        # make sure the source request uses it's container_info
        source_req.headers.pop('X-Backend-Storage-Policy-Index', None)
        source_req.path_info = quote(source_path)
        source_req.headers['X-Newest'] = 'true'
        source_resp = ssc_ctx.get_source_resp(source_req)

        # This gives middlewares a way to change the source; for example,
        # this lets you COPY a SLO manifest and have the new object be the
        # concatenation of the segments (like what a GET request gives
        # the client), not a copy of the manifest file.
        hook = req.environ.get(
            'swift.copy_hook',
            (lambda source_req, source_resp, req: source_resp))
        source_resp = hook(source_req, source_resp, req)

        if source_resp.content_length is None:
            # This indicates a transfer-encoding: chunked source object,
            # which currently only happens because there are more than
            # CONTAINER_LISTING_LIMIT segments in a segmented object. In
            # this case, we're going to refuse to do the server-side copy.

            # TODO: Check if above comment is true and
            # content_length can really be None.
            return HTTPRequestEntityTooLarge(request=req)

        if source_resp.content_length > MAX_FILE_SIZE:
            return HTTPRequestEntityTooLarge(request=req)

        return source_resp

    def copy(self, source_path, req, start_response):
        # GET the source object, bail out on error
        ssc_ctx = ServerSideCopyWebContext(self.wsgi_app, self.logger)
        source_resp = self._get_source_object(ssc_ctx, source_path, req)
        if source_resp.status_int >= HTTP_MULTIPLE_CHOICES:
            return source_resp(source_resp.environ, start_response)

        # Create a new Request object based on the original req instance.
        # This will preserve env and headers.
        sink_req = Request.blank(req.path_info,
                                 environ=req.environ, headers=req.headers)

        # Set data source, content length and etag for the PUT request
        sink_req.environ['wsgi.input'] = InputProxy(StringIO(source_resp.body))
        sink_req.content_length = source_resp.content_length
        sink_req.etag = source_resp.etag

        # We no longer need these headers
        sink_req.headers.pop('X-Copy-From', None)
        sink_req.headers.pop('X-Copy-From-Account', None)

        # If the copy request does not explicitly override content-type,
        # use the one present in the source object.
        if not req.headers.get('content-type'):
            sink_req.headers['Content-Type'] = \
                source_resp.headers['Content-Type']

        fresh_meta_flag = config_true_value(
            sink_req.headers.get('x-fresh-metadata', 'false'))

        if fresh_meta_flag or 'swift.post_as_copy' in sink_req.environ:
            # Post-as-copy: ignore new sysmeta, copy existing sysmeta
            condition = lambda k: is_sys_meta('object', k)
            remove_items(sink_req.headers, condition)
            copy_header_subset(source_resp, sink_req, condition)
        else:
            # Copy/update existing sysmeta and user meta
            copy_headers_into(source_resp, sink_req)
            # Copy/update new metadata provided in request if any
            copy_headers_into(req, sink_req)

        # Copy over x-static-large-object for POSTs and manifest copies
        if 'X-Static-Large-Object' in source_resp.headers and \
                (req.params.get('multipart-manifest') == 'get' or
                 'swift.post_as_copy' in req.environ):
            sink_req.headers['X-Static-Large-Object'] = \
                source_resp.headers['X-Static-Large-Object']

        # Create response headers for PUT response
        resp_headers = self._create_response_headers(source_path,
                                                     source_resp, sink_req)

        return ssc_ctx.send_put_req(sink_req, resp_headers, start_response)
