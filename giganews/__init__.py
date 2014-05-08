# -*- coding: utf-8 -*-
"""
Giganews Wrapper
~~~~~~~~~~~~~~~~~~~~~~~

usage:

>>> from giganews import NewsGroup
>>> group = NewsGroup('comp.sci.electronics')
>>> group.count
'331'

:copyright: (c) 2014 Internet Archive
:license: AGPL 3, see LICENSE for more details.

"""

__title__ = 'giganews'
__version__ = '0.0.1'
__author__ = 'Jake Johnson'
__license__ = 'AGPL 3'
__copyright__ = 'Copyright 2014 Internet Archive'

from .giganews import NewsGroup
