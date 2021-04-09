#!/usr/local/bin/python2.7
# vim: fileencoding=utf8
from __future__ import unicode_literals, division
"""
Author:    Alexander J. Quinn
Copyright: (c) 2012-2019 Alexander J. Quinn
License:   MIT License
"""

##############################################################################
#                                                                            #
# WARNING:  THIS FILE MAY BE SHARED WITH OTHER PROJECTS BY HARD LINKS        #
#                                                                            #
# As of 10/9/2012, it was linked to AskSheet, RFS, and s:/d/techref/snippets #
#                                                                            #
# To view hard links on Windows:  fsutil hardlink list safe_serialization.py #
# To make hard links on Windows:  mklink /h <link_name> <target_path>        #
#                                                                            #
##############################################################################


"""
This module does the same as repr + ast.literal_eval except that it also allows
datetime.datetime, datetime.date, datetime.time, and datetime.timedelta.
"""
from ast import parse, walk, Attribute, Call, Dict, Expr, Load, Module, Name, Num, Str, List, Tuple
import datetime, types

# To make eval more safe, we pass it a minimal globals/locals dict with only
# the datetime module.  To deny access to __builtins__, we must pass it an
# empty module in place of __builtins__.  According to the documentation for
# eval(..), if we didn't do that, then eval(..) would substitute in the old
# __builtin__ module.  (Note:  The normal module is called "__builtin__" but is
# under __builtins__ with an "s" in the globals() dict.)
_dummy_builtins_module = types.ModuleType(str("dummy_builtins_module"))
_minimal_globals_for_safe_eval = {
	"__builtins__" : _dummy_builtins_module,
	"datetime":datetime,
	"True" : True,
	"False" : False,
	"None" : None,
}
_safe_node_types = (Module, Expr, Dict, Str, Attribute, Num, Name, Load, List, Tuple)
_safe_datetime_module_attributes = ("datetime", "date", "time", "timedelta")
_simple_types = (int, float, bool, type(None), datetime.datetime, datetime.date, datetime.time, datetime.timedelta, type(b''.decode('ascii')), type(b''))

def safe_eval(s):
	# Credit:  This function is adapted from a snippet posted by "unutbu" at stackoverflow on 11/21/2010.
	#          http://stackoverflow.com/a/4236328/500022

	try:
		tree=parse(s)

	except SyntaxError:
		raise ValueError(s)

	for node in walk(tree):
		if isinstance(node, _safe_node_types):
			continue

		if isinstance(node,Call) and \
			   isinstance(node.func, Attribute) and \
			   node.func.value.id == "datetime" and \
			   node.func.attr in _safe_datetime_module_attributes:
			continue

		raise ValueError(s)

	# For extra caution, we will limit access to __builtins__ or anything else except the datetime module.
	return eval(s, _minimal_globals_for_safe_eval, _minimal_globals_for_safe_eval)

def safe_repr(o):
	if is_simple(o):
		return repr(o)
	else:
		raise ValueError("%r cannot be repr'd in a way that could be safely eval'd with this code."%(o,))

def is_simple(o):
	if isinstance(o, _simple_types):
		return True
	else:
		t = type(o)
		if (t is tuple) or (t is list):  # namedtuples are not allowed
			return all(is_simple(v) for v in o)
		elif t is dict:
			return all(is_simple(k) and is_simple(v) for (k,v) in o.iteritems())
		else:
			return False

def _test():
	import traceback, collections, datetime
	Foo = collections.namedtuple("Foo", ("a", "b", "c"))
	now = datetime.datetime.now()
	good_exprs = (
		None,
		3,
		3.1415,
		True,
		("i", "am", "a", "tuple"),
		["i", "am", "a", "list"],
		{"my_type":"dict", "my_name":"Tom"},
		{"nested_tuple":("tuple", "inside"), "nested_list":["list", "inside"], "nested_dict":{"dict":"inside"}},
		now,
		now.time(),
		now.date(),
		now - now,
	)
	bad_exprs = (
		Foo(1,2,3),
		{"namedtuple_as_value":Foo(1,2,3)},
	)

	test_num = 0  # will be 1-based
	for exprs,expect_success in ((good_exprs,True), (bad_exprs,False)):
		for expr in exprs:
			test_num += 1
			try:
				expr == safe_eval( safe_repr(expr) )
			except ValueError:
				got_success = False
			else:
				got_success = True

			if got_success and expect_success:
				print( "%2d. test success:  %r\n                   got no exception as expected"%(test_num, expr) )
			elif not got_success and not expect_success:
				print( "%2d. test success:  %r\n                   got ValueError as expected"%(test_num, expr) )
			elif expect_success:
				print( "%2d. test FAILURE:  %r\n                   expected success but got ValueError"%(test_num, expr) )
			else:
				print( "%2d. test FAILURE:  %r\n                   expected ValueError but got success"%(test_num, expr) )

	for exprs in (good_exprs, bad_exprs):
		for expr in exprs:
			print( repr(expr) )
			print(         "simple:    %r"%is_simple(expr) )
			try:
				r = safe_repr(expr)
				print(     "safe_repr: %s"%(r,))
				o = safe_eval(r)
				if o==expr:
					print( "status:    OK" )
				else:
					print( "status:    Doesn't match, original==%r, back-converted==%r"%(expr, o) )
			except Exception as e:
				print(     "status:    %r"%(e,) )
				#traceback.print_exc()
			print( "----" )

if __name__=="__main__":
	_test()
