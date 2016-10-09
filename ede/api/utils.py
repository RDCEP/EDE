#!/usr/bin/env python
# -*- coding: utf-8 -*-
from werkzeug.routing import BaseConverter


class ListConverter(BaseConverter):

    def to_python(self, value):
        return value.split(',')

    def to_url(self, values):
        return ','.join([BaseConverter.to_url(value) for value in values])


class IntListConverter(BaseConverter):

    def to_python(self, value):
        return [int(v) for v in value.split(',')]

    def to_url(self, values):
        return ','.join([BaseConverter.to_url(value) for value in values])

class RectangleConverter(BaseConverter):

    def to_python(self, value):
        vals = map(float, value.split(','))
        res = []
        i = 0
        while i < len(vals):
            res.append([vals[i], vals[i+1]])
            i += 2
        return res

    def to_url(self, values):
        return ','.join([BaseConverter.to_url(value) for value in values])


class ServerError(Exception):
    status_code = 500

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

class RasterExtractionException(Exception):
    """Represents an exception that can occur during the extraction of raster data from the DB.
    """

    def __init__(self, message):
        super(RasterExtractionException, self).__init__(message)


class RequestFormatException(Exception):
    """Is raised when the JSON body in a POST request is misformed.
    """

    def __init__(self, message):
        super(RequestFormatException, self).__init__(message)
