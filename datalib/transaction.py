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

from collections import defaultdict, Mapping
from itertools import ifilter, chain, groupby


class DependencyResolutionError(Exception):
    """Raised when problems with transaction-instruction could not be resolved
    by running other instructions in the transaction."""

    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return str(self.errors)


class TransactionAlreadyActiveError(Exception):
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
        """Activate interception of collection mutate request.

        Changes to a collection made while a transaction is active will
        not appear till the transaction is committed."""
        if not self.active:
            self.active = True
            self._instructions = defaultdict(list)
        else:
            raise TransactionAlreadyActiveError


    def add(self, type, instruction):
        """Add row-level instruction to transaction.

        >>> from datalib.hcollections import Collection
        >>> t = Transaction(Collection([]))
        >>> t.begin()
        >>> len(t)
        0
        >>> t.add('add_cols', lambda x: x)
        >>> len(t)
        1
        """
        self._instructions[type].append(instruction)

        # If session is not actively in use, apply atomically
        if not self.active:
            self.begin()
            self._instructions[type].append(instruction)
            self.commit()


    def rollback(self):
        """Erase list of transaction instructions and deactivate transaction.

        >>> from datalib.hcollections import Collection
        >>> t = Transaction(Collection([]))
        >>> t.begin()
        >>> t.add('add_cols', lambda x, y: x)
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
            if name not in self._instructions:
                continue
            try:
                commit_method(self, self._instructions[name])
            except (IndexError, ValueError), ex:
                errors.append((name, str(ex)))
                first_err_at = min(first_err_at, idx)

        if errors:
            if errors != last_errors:
                self.commit(first_err_at, errors)
            else:
                self.rollback()
                raise DependencyResolutionError(errors)
        else:
            self.active = False
            self.instructions = defaultdict(list)


    def _commit_filter(self, instructions):
        """Apply requested filters to collection."""
        col = iter(self._collection)
        for instruction in instructions:
            col = ifilter(instruction, col)

        col = list(col)
        if not col:
            self._collection.data = []
        else:
            if isinstance(col[0], Mapping):
                self._collection.data = [
                        [x[y] for y in self._collection.names] for x in col]
            else:
                self._collection.data = list(col)


    def _commit_group(self, instructions):
        """Apply requested  group operations to collection."""
        groupinst = instructions[0]

        if not callable(groupinst):
            groupfn = lambda x: x[groupinst]
            if hasattr(self._collection, 'names'):
                groupinst_key = self._collection.names.index(groupinst)
            else:
                groupinst_key = groupinst
        else:
            groupfn = groupinst

        child_collections = defaultdict(None)
        record_length = len(self._collection[0])
        records = []
        for key, sub in groupby(self._collection, groupfn):
            sub = self._collection.factory(sub)
            group_record = [None] * record_length
            if not callable(groupinst):
                group_record[groupinst_key] = sub[0][groupinst]
            records.append(group_record)
            child_collections[len(child_collections)] = sub

        self._collection.data = records
        self._collection._child_collections = child_collections

        instructions[:] = instructions[1:]


    def _commit_new_cols(self, instructions):
        """Add calculated and formatted columns to collection."""
        for idx, row in enumerate(self._collection):
            if isinstance(row, Mapping):
                row = self._collection.data[idx]
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

