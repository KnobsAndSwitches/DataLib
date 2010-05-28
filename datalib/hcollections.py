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

"""Homogeneous data collections."""

from datalib.transaction import Transaction


class Collection(object):
    """Basic data-collection.
    
    Example:
    >>> Collection(((1,2,3), (2,3,4)))
    <Collection 2 rows, 3 columns>
    >>> Collection([])
    <Collection Empty>
    """

    def __init__(self, data, **kwargs):
        self.data = data
        self.transaction = Transaction(self)

        # Add filters
        def _common_kwarg_handling():
            if 'filter' in kwargs:
                for filter_fn in kwargs['filter']:
                    self.filter(filter_fn)

        self._handle_kwargs(_common_kwarg_handling, **kwargs)


    def __len__(self):
        return len(self.data)


    def __iter__(self):
        return iter(self.data)


    def __getitem__(self, key):
        return self.data[key]


    def __enter__(self):
        self.transaction.begin()


    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type:
            self.transaction.commit()
        else:
            self.transaction.rollback()


    def __repr__(self):
        if self.data:
            return ("<Collection %s rows, %s columns>" 
                    % (len(self.data), len(self.data[0])))
        else:
            return "<Collection Empty>"


    def add_formatted_column(self, fmt):
        """Add new column defined by given format string.

        The python format specification is used to create the resulting 
        columns. (http://docs.python.org/library/string.html#formatstrings)

        >>> col = Collection((('a', 'b'), ('c', 'd')))
        >>> col.add_formatted_column('{0}, {1}')
        >>> col[0][2]
        'a, b'
        >>> col[1][2]
        'c, d'
        """
        def _do_format(row, collection):
            row.append(fmt.format(*row))

        self.transaction.add('new_cols', _do_format)


    def filter(self, fn):
        """Filter rows that match given function."""
        self.transaction.add('filter', fn)


    def _handle_kwargs(self, common_kwarg_handling, **kwargs):
        """Handle kwargs passed in on __init__."""
        with self:
            common_kwarg_handling()
            if 'formatted_columns' in kwargs:
                for col in kwargs['formatted_columns']:
                    self.add_formatted_column(col)


class NamedCollection(Collection):
    """Name-based data-collection.

    Example:
    >>> NamedCollection(('a', 'b', 'c'), ((1,2,3), (4,5,6)))
    <NamedCollection 2 rows, 3 columns>
    >>> NamedCollection([], [])
    <NamedCollection Empty>
    """
    
    def __init__(self, names, data, **kwargs):
        self.names = list(names)
        super(NamedCollection, self).__init__(data, **kwargs)


    def __repr__(self):
        if self.data:
            return ("<NamedCollection %s rows, %s columns>" 
                    % (len(self.data), len(self.data[0])))
        else:
            return "<NamedCollection Empty>"


    def __iter__(self):
        """Return iterator yielding a dictionary per row.

        >>> col = NamedCollection(('a', 'b'), ((1,2),(3,4)))
        >>> list(col.__iter__())[0]
        {'a': 1, 'b': 2}
        """
        return (dict(zip(self.names, x)) for x in self.data)

    
    def add_formatted_column(self, name, fmt):
        """Add new named formatted column.

        >>> col = NamedCollection(('a', 'b'), (('foo', 'bar'),))
        >>> col.add_formatted_column('c', '{b}, {a}')
        >>> col[0]
        ['foo', 'bar', 'bar, foo']
        """
        for idx, n in enumerate(self.names):
            fmt = fmt.replace('{%s}' % n, '{%s}' % idx)
        def _do_format(row, collection):
            collection.names.append(name)
            row.append(fmt.format(*row))

        self.transaction.add('new_cols', _do_format)


    def _handle_kwargs(self, common_kwarg_handling, **kwargs):
        """Handle kwargs passed in on __init__."""
        with self:
            common_kwarg_handling()
            if 'formatted_columns' in kwargs:
                for col in kwargs['formatted_columns']:
                    self.add_formatted_column(*col)

