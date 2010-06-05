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

"""Test Named Record."""

from datalib.records import NamedRecord


DATA_A = {'a': 1, 'b': 2, 'c': 3}
DATA_B = {'a': 2, 'b': 3, 'c': 4}


def test_create():
    assert dict(NamedRecord(DATA_A)) == dict(DATA_A)


def test_hash():
    DATA_C = dict(DATA_A)

    r1 = NamedRecord(DATA_A)
    r2 = NamedRecord(DATA_B)
    r3 = NamedRecord(DATA_C)

    assert hash(r1) != hash(r2)
    assert hash(r1) == hash(r3)

