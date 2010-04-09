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

"""Collection change transaction."""


INSTRUCTION_ROW = 0
INSTRUCTION_COL = 1


class TransactionAlreadyActiveError(Exception):
    pass


class TransactionNotActiveError(Exception):
    pass


class Transaction(object):
    """Collection transaction."""

    def __init__(self, collection):
        self.active = False
        self._collection = collection
        self._instructions = []


    def __len__(self):
        return len(self._instructions)


    def begin(self):
        """Activate interception of datagrid mutate request.

        Changes to a collection made while a transaction is active will
        not appear till the transaction is committed."""
        if not self.active:
            self.active = True
            self._instructions = []
        else:
            raise TransactionAlreadyActiveError


    def add_row_instruction(self, instruction):
        """Add row-level instruction to todo list on commit.

        >>> from datalib.collections import Collection
        >>> t = Transaction(Collection([]))
        >>> t.begin()
        >>> len(t)
        0
        >>> t.add_row_instruction(lambda x: x)
        >>> len(t)
        1
        """
        if self.active:
            self._instructions.append((INSTRUCTION_ROW, instruction))
        else:
            raise TransactionNotActiveError


    def commit(self):
        """Apply requested instructions to bound collection."""
        # Make sure our dataset is mutable
        self._collection.data = list(self._collection.data)

        for idx, row in enumerate(self._collection):
            row = list(row) # make sure our row is mutable
            for inst_type, inst_fun in self._instructions:
                if inst_type == INSTRUCTION_ROW:
                    inst_fun(row, self._collection)
                    self._collection.data[idx] = row

        self.active = False


    def rollback(self):
        """Erase list of transaction instructions and deactivate transaction.

        >>> from datalib.collections import Collection
        >>> t = Transaction(Collection([]))
        >>> t.begin()
        >>> t.add_row_instruction(lambda x, y: x)
        >>> len(t)
        1
        >>> t.rollback()
        >>> t.active
        False
        >>> len(t)
        0
        """
        self.active = False
        self._instructions = []

