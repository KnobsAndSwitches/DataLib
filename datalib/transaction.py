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

from collections import defaultdict


class DependencyResolutionError(Exception):
    pass


class TransactionAlreadyActiveError(Exception):
    pass


class TransactionNotActiveError(Exception):
    pass


class Transaction(object):
    """Collection transaction."""

    def __init__(self, collection):
        self.active = False
        self._collection = collection
        self._instructions = defaultdict(list)


    def __len__(self):
        return len(self._instructions)


    def begin(self):
        """Activate interception of datagrid mutate request.

        Changes to a collection made while a transaction is active will
        not appear till the transaction is committed."""
        if not self.active:
            self.active = True
            self._instructions = defaultdict(list)
        else:
            raise TransactionAlreadyActiveError


    def add(self, type, instruction):
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
            self._instructions[type].append(instruction)
        else:
            raise TransactionNotActiveError


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
        self._instructions = defaultdict(list)


    def commit(self, start_at=0, last_errors=None):
        """Apply requested instructions to bound collection."""
        # Make sure our dataset is mutable
        self._collection.data = list(self._collection.data)
        errors, first_err_at = [], 99
        
        for idx, (name, commit_method) in enumerate(self.commit_methods):
            try:
                commit_method(self, self._instructions[name])
            except IndexError, ex:
                errors.append(ex)
                first_err_at = min(first_err_at, idx)

        if errors:
            if errors != last_errors:
                self.commit(first_err_at, errors)
            else:
                raise DependencyResolutionError
        else:
            self.active = False


    def _commit_filter(self, instructions):
        """Apply requested filters to collection."""
        pass


    def _commit_group(self, instructions):
        """Apply requested group opperations to collection."""
        pass


    def _commit_new_cols(self, instructions):
        """Add calculated and formatted columns to collection."""
        for idx, row in enumerate(self._collection):
            row = list(row) # make sure our row is mutable
            for instruction in instructions:
                instruction(row, self._collection)
            self._collection.data[idx] = row


    def _commit_aggregate(self, instructions):
        """Apply column aggregations."""
        pass


    def _commit_sort(self, instructions):
        """Apply sort rules."""
        pass


    commit_methods = (
            ('filter', _commit_filter),
            ('group', _commit_group),
            ('new_cols', _commit_new_cols),
            ('aggregate', _commit_aggregate),
            ('sort', _commit_sort),)

