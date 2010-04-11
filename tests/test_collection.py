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

from py.test import raises

from datalib.hcollections import Collection


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

    col = Collection(STRING_DATA, formatted_columns=('{2} {1} {0}',))
    assert col[0][3] == 'baz bar foo'


def test_transaction_verbose():
    col = Collection(STRING_DATA)
    assert col.transaction.active == False

    col.transaction.begin()
    assert col.transaction.active == True
    assert len(col.transaction) == 0

    col.add_formatted_column('{0}, {1}')
    assert len(col.transaction) == 1

    col.transaction.rollback()
    assert len(col.transaction) == 0
    assert col.transaction.active == False

    col.transaction.begin()
    col.add_formatted_column('{0}, {1}')
    col.transaction.commit()
    assert col[0][3] == 'foo, bar'


def test_transaction_with():
    col = Collection(STRING_DATA)

    with col:
        col.add_formatted_column('{0}, {1}')
        col.transaction.rollback()
    raises(IndexError, 'col[0][3]')

    with col:
        col.add_formatted_column('{0}, {1}')
    assert col[0][3] == 'foo, bar'

