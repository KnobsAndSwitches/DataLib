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

"""Test Collection."""

from datalib.collections import Collection


BASIC_DATA = ((1,2,3),(4,5,6))
STRING_DATA = (('foo', 'bar', 'baz'),)


def test_create():
    col = Collection(BASIC_DATA)

    for idx, row in enumerate(col):
        assert list(row) == list(BASIC_DATA[idx])


def test_formatted_columns():
    col = Collection(STRING_DATA)
    col.add_formatted_column('{2} {0} {1}')
    assert col[0][3] == 'baz foo bar'

    col = Collection(STRING_DATA, formatted_columns=('{c} {b} {a}',))
    assert col[0][3] == 'baz bar foo'

