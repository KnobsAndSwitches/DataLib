# Copyright (C) 2010 Adam Wagner <awagner83@gmail.com>, 
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published 
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Collection records."""


class Record(list):
    """Index based collection record. with support for nested collections.

    Example:
    >>> Record([1, 2, 3])
    [1, 2, 3]
    >>> r = Record([1, 2, 3], children='testing')
    >>> r.children
    'testing'
    """
    
    def __init__(self, iterable, children=None):
        super(Record, self).__init__(iterable)
        self.children = children

    def __hash__(self):
        if not self:
            return 0
        self_str = ''.join(str(x) for x in self)
        value = ord(self_str[0]) << 7
        for char in self_str:
            value = c_mul(1000003, value) ^ ord(char)
        value = value ^ len(self_str)
        if value == -1:
            value = -2
        return value


class NamedRecord(dict):
    """Key based collection record, with support for nested collections.

    Example:
    >>> NamedRecord({'a': 1, 'b': 2, 'c': 3}) == {'a': 1, 'b': 2, 'c': 3}
    True
    >>> r = NamedRecord({'a': 1, 'b': 2, 'c': 3}, children='testing')
    >>> r.children
    'testing'
    """

    def __init__(self, mapping, children=None):
        super(NamedRecord, self).__init__(mapping)
        self.children = children

    def __hash__(self):
        if not self:
            return 0
        self_str = ''.join(x + str(self[x]) for x in sorted(self))
        value = ord(self_str[0]) << 7
        for char in self_str:
            value = c_mul(1000003, value) ^ ord(char)
        value = value ^ len(self_str)
        if value == -1:
            value = -2
        return value


def c_mul(a, b):
    return eval(hex((long(a) * b) & 0xFFFFFFFFL)[:-1])


