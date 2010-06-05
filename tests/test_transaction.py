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

from datalib.transaction import TransactionAlreadyActiveError, Transaction
from datalib.hcollections import Collection


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

