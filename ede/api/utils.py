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
        print len(vals)
        res = []
        i = 0
        while i < len(vals):
            res.append([vals[i], vals[i+1]])
            i += 2
        return res

    def to_url(self, values):
        return ','.join([BaseConverter.to_url(value) for value in values])