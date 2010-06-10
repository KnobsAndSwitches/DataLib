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

"""Transaction tests."""

from py.test import raises

from datalib.transaction import (DependencyResolutionError, 
        TransactionAlreadyActiveError, Transaction)
from datalib.hcollections import Collection, NamedCollection


def test_begin_twice():
    transaction = Transaction(Collection([]))
    transaction.begin()
    raises(TransactionAlreadyActiveError, transaction.begin)

    # no exception raised if transaction is already finished
    transaction.rollback()
    transaction.begin()


def test_commit_filter():
    ref_collection = Collection([(1,2),(2,3),(3,4)])

    # test impossible filter
    col1 = ref_collection.factory(ref_collection)
    t1 = Transaction(col1)
    t1._commit_filter([lambda x: False])
    assert col1.data == []

    # something more reasonable
    col2 = ref_collection.factory(ref_collection)
    t2 = Transaction(col2)
    t2._commit_filter([lambda x: x[0] > 2])
    assert len(col2) == 1
    assert set(col2) < set(ref_collection)


def test_commit_group():
    ref_collection = Collection([(1,2),(1,3),(2,4)])

    # index-based grouping
    col = ref_collection.factory(ref_collection)
    transaction = Transaction(col)
    transaction._commit_group([0])
    assert len(col) == 2
    assert set(col[0].children) < set(ref_collection)

    # aka
    col2 = ref_collection.factory(ref_collection)
    col2.transaction.add('group', 0)
    assert set(col2) == set(col)

    # fn based grouping
    col = ref_collection.factory(ref_collection)
    transaction = Transaction(col)
    transaction._commit_group([lambda x: x[1]])
    assert len(col) == 3
    assert set(col[0].children) < set(ref_collection)

    # aka
    col2 = ref_collection.factory(ref_collection)
    col2.transaction.add('group', lambda x: x[1])
    assert set(col2) == set(col)


def test_commit_error():
    transaction = Transaction(Collection([[1]]))

    # test long way
    transaction.begin()
    transaction.add('group', 1) # <- Column "1" doesn't exist.
    raises(DependencyResolutionError, transaction.commit)

    # shorter test (but this does the same thing)
    raises(DependencyResolutionError, transaction.add, 'group', 1)

    # check exception error message
    try:
        transaction.add('group', 1)
    except DependencyResolutionError, e:
        assert str(e) == "[('group', 'list index out of range')]"

    # check for errors in named-collections as well
    transaction = Transaction(NamedCollection(['a'], [[1]]))
    raises(DependencyResolutionError, transaction.add, 'group', 'b')


def test_commit_new_cols():
    col = Collection([[1]])
    col.transaction.add('new_cols', lambda x, y: 'test')
    assert len(col[0]) == 2


def test_error_correction():
    col = Collection([[1]])
    col.transaction.begin()
    col.transaction.add('group', 1)
    col.transaction.add('new_cols', lambda x, y: 'test')
    col.transaction.commit()

    assert len(col[0]) == 2
    assert len(col[0].children) == 1

