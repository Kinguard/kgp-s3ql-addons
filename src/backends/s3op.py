'''
backends/gs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, QuietError # Ensure use of custom logger class
from . import s3c
from .s3c import  HTTPError, S3Error
import threading
from pylibopi import AuthLogin

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101,W0201

log = logging.getLogger(__name__)

class Backend(s3c.Backend):
    """A backend to store data in OpenProducts Backend

    This class uses standard HTTP connections to connect to OpenProducts Servers

    The backend guarantees immediate get consistency and eventual list
    consistency.
    """

    # We don't want to request an access token for each instance,
    # because there is a limit on the total number of valid tokens.
    # This class variable holds the mapping from refresh tokens to
    # access tokens.
    access_token = dict()
    _refresh_lock = threading.Lock()


    def __init__(self, options):
        self.hdr_prefix = 'x-amz-'
        super().__init__(options)


    def _authorize_request(self, method, path, headers, subres, query_string):
        '''Add authorization information to *headers*'''

        headers['token'] = self.access_token['token']


    def _do_request(self, method, path, subres=None, query_string=None,
                    headers=None, body=None):

        # If we have an access token, try to use it.
        token = self.access_token.get('token')
        if token is not None:
            try:
                log.debug("Trying to use existing token")
                return super()._do_request(method, path, subres=subres, headers=headers,
                                           query_string=query_string, body=body)
            except HTTPError as exc:
                if exc.status != 401:
                    raise
            except S3Error as exc:
                if exc.code != 'AuthenticationRequired':
                    raise

        # If we reach this point, then the access token must have
        # expired, so we try to get a new one. We use a lock to prevent
        # multiple threads from refreshing the token simultaneously.
        log.debug("Missing token or token has expired")
        with self._refresh_lock:
            # Don't refresh if another thread has already done so while
            # we waited for the lock.
            #if token is None or self.access_token.get(self.password, None) == token:
            if True:
                try:
                    self.access_token['token'] = AuthLogin()
                except Exception as e:
                    log.debug("Failed to authenticate to OP servers")
                    raise

        # Reset body, so we can resend the request with the new access token
        if body and not isinstance(body, (bytes, bytearray, memoryview)):
            body.seek(0)

        # Try request again. If this still fails, propagate the error
        # (because we have just refreshed the access token).
        # FIXME: We can't rely on this if e.g. the system hibernated
        # after refreshing the token, but before reaching this line.
        return super()._do_request(method, path, subres=subres, headers=headers,
                                   query_string=query_string, body=body)

    #@retry
    def _list_page(self, prefix, page_token=None, batch_size=1000):

        # We can get at most 1000 keys at a time, so there's no need
        # to bother with streaming.
        query_string = { 'prefix': prefix, 'max-keys': str(batch_size) }
        if page_token:
            query_string['marker'] = page_token
        log.debug("OP request")

        resp = self._do_request('GET', '/', query_string=query_string)
        log.debug(resp)
        log.debug("OP response done")

        if not s3c.XML_CONTENT_RE.match(resp.headers['Content-Type']):
            raise RuntimeError('unexpected content type: %s' %
                               resp.headers['Content-Type'])

        body = self.conn.readall()
        log.debug(body)
        etree = s3c.ElementTree.fromstring(body)
        root_xmlns_uri = s3c._tag_xmlns_uri(etree)
        log.debug(root_xmlns_uri)
        if root_xmlns_uri is None:
            root_xmlns_prefix = ''
        else:
            # Validate the XML namespace
            root_xmlns_prefix = '{%s}' % (root_xmlns_uri, )
            if root_xmlns_prefix != self.xml_ns_prefix:
                log.error('Unexpected server reply to list operation:\n%s',
                          self._dump_response(resp, body=body))
                raise RuntimeError('List response has unknown namespace')

        names = [ x.findtext(root_xmlns_prefix + 'Key')
                  for x in etree.findall(root_xmlns_prefix + 'Contents') ]

        is_truncated = etree.find(root_xmlns_prefix + 'IsTruncated')
        if is_truncated.text == 'false':
            page_token = None
        else:
            page_token = names[-1]

        return (names, page_token)

