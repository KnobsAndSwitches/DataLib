# Copyright (C) 2010 Adam Wagner <awagner83@gmail.com>, 
#                    Kenny Parnell <k.parnell@gmail.com>
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

"""Test NamedCollection."""

from datalib.hcollections import NamedCollection


BASIC_DATA = ('a', 'b', 'c'), [[1,2,3],[4,5,6]]
STRING_DATA = ('a', 'b', 'c'), (('foo', 'bar', 'baz'),)


def test_create_named():
    names, data = BASIC_DATA
    col = NamedCollection(names, data)

    assert list(col.names) == list(names)
    assert list(col.data) == list(data)

    for idx, row in enumerate(col.data):
        assert list(row) == list(data[idx])


def test_dict_rows():
    names, data = BASIC_DATA
    col = NamedCollection(names, data)

    for idx, row in enumerate(col):
        row_names, row_values = zip(*row.iteritems())
        assert set(row_names) == set(names)
        assert set(row_values) == set(data[idx])


def test_formatted_columns():
    names, data = STRING_DATA
    col = NamedCollection(names, data)

    col.add_formatted_column('d', '{c} {a} {b}')
    assert list(col)[0]['d'] == 'baz foo bar'

    col = NamedCollection(names, data, 
            formatted_columns=(('d', '{c} {b} {a}'),))
    assert list(col)[0]['d'] == 'baz bar foo'


def test_filter():
    col = NamedCollection(*BASIC_DATA)
    col.filter(lambda x: x['a'] < 2)
    assert len(col) == 1

    col = NamedCollection(*BASIC_DATA, filter=(lambda x: x['a'] < 2,))
    assert len(col) == 1

