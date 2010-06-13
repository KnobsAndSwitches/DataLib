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
STRING_DATA = ('a', 'b', 'c'), (('foo', 'bar', 'baz'),('zip','zap','biff'))
GROUP_DATA = ('a', 'b'), ((1, 2), (1, 3), (1, 4), (2, 1))


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
    assert len(col.names) == 4

    col = NamedCollection(names, data, 
            formatted_columns=(('d', '{c} {b} {a}'),))
    assert list(col)[0]['d'] == 'baz bar foo'


def test_calculated_columns():
    col = NamedCollection(*BASIC_DATA)
    col.add_calculated_column("d", "{a} + {b}")
    assert col[0]['d'] == 3

    col = NamedCollection(*BASIC_DATA, calculated_columns=(('d', '{a} + {b}',),))
    assert col[1]['d'] == 9


def test_filter():
    col = NamedCollection(*BASIC_DATA)
    col.filter(lambda x: x['a'] < 2)
    assert len(col) == 1

    col = NamedCollection(*BASIC_DATA, filter=(lambda x: x['a'] < 2,))
    assert len(col) == 1


def test_group():
    col_ref = NamedCollection(*GROUP_DATA)
    col1 = NamedCollection(*GROUP_DATA)
    col1.group(['a'])
    col2 = NamedCollection(*GROUP_DATA, group=['a'])
    
    for col in [col1, col2]:
        assert len(col) == 2
        assert col[0]['a'] == 1
        assert col[1]['a'] == 2
        assert len(col[0].children) == 3
        assert type(col) == type(col[0].children)
        assert set(col[0].children) <= set(col_ref)
        for row in col:
            assert len(row) == 2

