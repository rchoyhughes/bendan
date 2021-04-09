#!/usr/local/bin/python2.7
# vim: set fileencoding=utf8 noexpandtab tabstop=4 shiftwidth=4:   # boilerplate

"""
Author:    Alexander J. Quinn
Copyright: (c) 2012-2019 Alexander J. Quinn
License:   MIT License
"""
from __future__ import division as _, unicode_literals as _; del _ # boilerplate

import os, sys

_in_base    = lambda *parts: os.path.join(PATH_BASE, *parts)  # creates absolute path in project base
_DATA_DIR   = "data"
_STATIC_DIR = "static"

PATH_BASE = os.path.dirname(os.path.abspath(__file__))
PATH_DATABASE = _in_base(_DATA_DIR, "database.db")
PATH_CONSENT_HTML = _in_base(_STATIC_DIR, "consent_form.html")

del os, sys  # keep namespace clean
