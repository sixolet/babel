# -*- coding: utf-8 -*-
"""
    babel.localedata
    ~~~~~~~~~~~~~~~~

    Low-level locale data access.

    :note: The `Locale` class, which uses this module under the hood, provides a
           more convenient interface for accessing the locale data.

    :copyright: (c) 2013 by the Babel Team.
    :license: BSD, see LICENSE for more details.
"""

import os
import threading
from collections import MutableMapping, Mapping
from itertools import chain

from babel._compat import pickle


_cache = {}
_cache_lock = threading.RLock()
_dirname = os.path.join(os.path.dirname(__file__), 'locale-data')


def normalize_locale(name):
    """Normalize a locale ID by stripping spaces and apply proper casing.

    Returns the normalized locale ID string or `None` if the ID is not
    recognized.
    """
    name = name.strip().lower()
    for locale_id in chain.from_iterable([_cache, locale_identifiers()]):
        if name == locale_id.lower():
            return locale_id


def exists(name):
    """Check whether locale data is available for the given locale.

    Returns `True` if it exists, `False` otherwise.

    :param name: the locale identifier string
    """
    if name in _cache:
        return True
    file_found = os.path.exists(os.path.join(_dirname, '%s.dat' % name))
    return True if file_found else bool(normalize_locale(name))


def locale_identifiers():
    """Return a list of all locale identifiers for which locale data is
    available.

    .. versionadded:: 0.8.1

    :return: a list of locale identifiers (strings)
    """
    return [stem for stem, extension in [
        os.path.splitext(filename) for filename in os.listdir(_dirname)
    ] if extension == '.dat' and stem != 'root']


def load(name, merge_inherited=True):
    """Load the locale data for the given locale.

    The locale data is a dictionary that contains much of the data defined by
    the Common Locale Data Repository (CLDR). This data is stored as a
    collection of pickle files inside the ``babel`` package.

    >>> d = load('en_US')
    >>> d['languages']['sv']
    u'Swedish'

    Note that the results are cached, and subsequent requests for the same
    locale return the same dictionary:

    >>> d1 = load('en_US')
    >>> d2 = load('en_US')
    >>> d1 is d2
    True

    :param name: the locale identifier string (or "root")
    :param merge_inherited: whether the inherited data should be merged into
                            the data of the requested locale
    :raise `IOError`: if no locale data file is found for the given locale
                      identifer, or one of the locales it inherits from
    """
    _cache_lock.acquire()
    try:
        data = _cache.get(name)
        if not data:
            # Load inherited data
            filename = os.path.join(_dirname, '%s.dat' % name)
            with open(filename, 'rb') as fileobj:
                file_data = pickle.load(fileobj)
            load_inherited = (name != 'root' and merge_inherited)
            if load_inherited:
                from babel.core import get_global
                parent = get_global('parent_exceptions').get(name)
                if not parent:
                    parts = name.split('_')
                    if len(parts) == 1:
                        parent = 'root'
                    else:
                        parent = '_'.join(parts[:-1])
                data = merged(load(parent), file_data)
            else:
                data = file_data
            _cache[name] = data
        return data
    finally:
        _cache_lock.release()

# Using MergedDictView instead of a copying merge strategy for larger dicts uses
# somewhere between 40% and 60% of the memory, depending on how many of the
# fields in the locale dict structure you ever access.  Access time becomes
# somewhat greater.
class MergedDictView(Mapping):
    """Represents a merging of two constituent mappings, according to the same merge
    rules as `merge`, but does not copy either dict.  For the contents of the
    resulting mapping, see `merge`.

    The behavior of MergedDictView is undefined if either `left` or `right` is
    modified after creation in any way other than:
    * resolving aliases
    * replacing elements with their LocaleDataDict equivalents.
    """

    __slots__ = ('_left', '_right')

    def __init__(self, left, right):
        """Initialize the MergedDictView from the two given mappings
        """
        # Since a MergedDictView is just likely going to get wrapped in another
        # LocaleDataDict, unwrap any LocaleDataDicts before using them as either
        # side.
        if type(left) is LocaleDataDict:
            left = left._data
        if type(right) is LocaleDataDict:
            right = right._data
        self._left = left
        self._right = right

    def __getitem__(self, key):
        # This is the by-reference equivalent of merge()
        right_val = self._right.get(key)
        if right_val is not None:
            left_val = self._left.get(key)
            if left_val is not None:
                if is_mapping(right_val):
                    # merge code
                    left_type = type(left_val)
                    if left_type is Alias:
                        return (left_val, right_val)
                    elif left_type is tuple:
                        alias, others = left_val
                        return (alias, merged(others, right_val))
                    else:
                        # Because of the structure of the dicts we're dealing
                        # with, it's never the case that we're looking at a
                        # mapping on the right and a scalar on the left.
                        return merged(left_val, right_val)

                else:
                    return right_val
            else:
                return right_val
        return self._left[key]

    def __iter__(self):
        return iter(frozenset(self._left.keys()) | frozenset(self._right.keys()))

    def __len__(self):
        return len(frozenset(self._left.keys()) | frozenset(self._right.keys()))

    def copy(self):
        return {k: v for k, v in self.iteritems()}

# The threshold for when it's more efficient to copy the dicts than refer to
# them.  The value of 7 was picked by loading and then walking every locale;
# runtime and Mb taken by the load and walk.

# 5:        1m42.509s 121.4/22.8
# 6:        1m39.808s 121.4/22.2
# 7:        1m41.369s 121.5/20.5
# 8:        1m40.822s 121.4/21.4
# 10:       1m44.447s 121.5/20.6

# Using no reference-merged dicts and only copying, we actually have overall
# faster performance but memory suffers greatly:
# infinity: 1m18.706s 192.4/209.0
COPY_THRESHOLD = 7

def merged(dict1, dict2, require_mutable=False):
    """Return a new dict that is dict2 merged into dict1, without mutating either
    dict"""
    len1 = len(dict1)
    len2 = len(dict2)
    if len1 == 0 and len2 == 0:
        return {}
    elif len2 == 0:
        ret = dict1
    elif len1 == 0:
        ret = dict2
    elif len1 + len2 < COPY_THRESHOLD:
        ret = dict1.copy()
        merge(ret, dict2)
    else:
        ret = MergedDictView(dict1, dict2)
    if require_mutable and not is_mutable_mapping(ret):
        ret = MutableDictView(ret)
    return ret


def merge(dict1, dict2):
    """Merge the data from `dict2` into the `dict1` dictionary, recursively merging
    any nested dictionaries.

    >>> d = {1: 'foo', 3: 'baz'}
    >>> merge(d, {1: 'Foo', 2: 'Bar'})
    >>> sorted(d.items())
    [(1, 'Foo'), (2, 'Bar'), (3, 'baz')]

    :param dict1: the dictionary to merge into
    :param dict2: the dictionary containing the data that should be merged

    """
    for key, val2 in dict2.items():
        if val2 is not None:
            val1 = dict1.get(key)
            if is_mapping(val2):
                if val1 is None:
                    val1 = {}
                if isinstance(val1, Alias):
                    val1 = (val1, val2)
                elif isinstance(val1, tuple):
                    alias, others = val1
                    val1 = (alias, merged(others, val2))
                else:
                    val1 = merged(val1, val2)
            else:
                val1 = val2
            dict1[key] = val1


class Alias(object):
    """Representation of an alias in the locale data.

    An alias is a value that refers to some other part of the locale data,
    as specified by the `keys`.
    """

    def __init__(self, keys):
        self.keys = tuple(keys)

    def __repr__(self):
        return '<%s %r>' % (type(self).__name__, self.keys)

    def resolve(self, data):
        """Resolve the alias based on the given data.

        This is done recursively, so if one alias resolves to a second alias,
        that second alias will also be resolved.

        :param data: the locale data
        :type data: `dict`
        """
        base = data
        for key in self.keys:
            data = data[key]
        t = type(data)
        if t is Alias:
            data = data.resolve(base)
        elif t is tuple:
            alias, others = data
            data = alias.resolve(base)
        return data

class MutableDictView(MutableMapping):
    __slots__ = ('_overrides', '_deleted_keys', '_base')

    def __init__(self, base):
        self._base = base
        self._deleted_keys = None
        self._overrides = None

    def __keys(self):
        k = set(self._base.keys())
        if self._overrides is not None:
            k |= set(self._overrides.keys())
        if self._deleted_keys is not None:
            k -= self._deleted_keys
        return k

    def __len__(self):
        return len(self.__keys())

    def __iter__(self):
        return iter(self.__keys())

    def __getitem__(self, key):
        if self._deleted_keys is not None and key in self._deleted_keys:
            raise KeyError(key)
        if self._overrides is not None and key in self._overrides:
            return self._overrides[key]
        return self._base[key]

    def __setitem__(self, key, value):
        if self._overrides is None:
            self._overrides = {}
        self._overrides[key] = value
        if self._deleted_keys is not None:
            self._deleted_keys.discard(key)

    def __delitem__(self, key):
        if self._deleted_keys is None:
            self._deleted_keys = set()
        self._deleted_keys.add(key)

    def copy(self):
        return MutableDictView(self)

class LocaleDataDict(MutableMapping):
    """Dictionary wrapper that automatically resolves aliases to the actual
    values.
    """

    __slots__ = ('_data', 'base')

    def __init__(self, data, base=None):
        if not is_mutable_mapping(data):
            data = MutableDictView(data)
        self._data = data
        if base is None:
            base = data
        self.base = base

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __getitem__(self, key):
        orig = val = self._data[key]
        if type(val) is LocaleDataDict:
            return val
        if type(val) is Alias:  # resolve an alias
            val = val.resolve(self.base)
        if type(val) is tuple:  # Merge a partial dict with an alias
            alias, others = val
            val = merged(alias.resolve(self.base), others, require_mutable=True)
        if is_mapping(val):
            # Return a nested alias-resolving dict, and store it for the next
            # time.  It was an alias that got looked up, or a raw dict.
            val = LocaleDataDict(val, base=self.base)
            self._data[key] = val
        return val

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]

    def copy(self):
        return LocaleDataDict(self._data.copy(), base=self.base)

_mappings = tuple(id(x) for x in
                  (LocaleDataDict, dict, MergedDictView, MutableDictView))

_mutable_mappings = tuple(id(x) for x in
                          (dict, LocaleDataDict, MutableDictView))

def is_mapping(d):
    """Return True if d is one of the kinds of mapping you're ever going to see
    in a locale dict structure.  Do so as fast as possible.

    """
    # This provides about a 2x speedup in loading & accessing locale data overall
    t = id(type(d))
    return t in _mappings

def is_mutable_mapping(d):
    """Return True if d is one of the kinds of mutable mapping you're ever going to
    see in a locale dict structure.  Do so as fast as possible
    """
    t = id(type(d))
    return t in _mutable_mappings
