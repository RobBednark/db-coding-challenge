#!/usr/bin/env python3
"""run_db.py - datastore for importing and querying

Usage:
    run_db.py import [--delete] <filename>
    run_db.py query [-s FIELDS] [-f FILTERS] [-o ORDERS] [-g FIELD]

Arguments:
    <filename>  filename containing csv data to import

Options:
    --delete    Delete the existing datastore before importing the file.
    -f FILTERS  FIELDS is a list of comma-separated field=value pairs to filter
                by. e.g., -f STB=stb1
    -g FIELD    Group-by the specified FIELD column. e.g., -g TITLE
    -o ORDERS   ORDERS is a list of comma-separated fields to order by, in the
                order in which they are specified.  e.g., -o DATE,TITLE
    -s FIELDS   FIELDS is a list of comma-separated fields to display in the
                output, with optional aggregation functions
                (min,max,count,collect,sum).
                e.g., -s DATE,TITLE:collect
                If aggregate functions are specified, they apply to the rows
                grouped with the -g FIELD option.
                If -s is not specified, then show all fields.

Commands:
    import      Import the csv data in <filename> into the datastore.
    query       Query the datastore.
"""

import sys
import docopt
from datastore import import_data
from query import query


def run(args, outfile_obj=sys.stdout):
    if args.get('import'):
        try:
            infile_obj = open(args['<filename>'], 'r')
        except Exception as exception:
            print('Error opening file [%s] for reading, exception = [%s] [%s]'
                  % (args['<filename>'], type(exception).__name__, exception))
            sys.exit(1)
        num = import_data(infile_obj, delete_first=args['--delete'])
        print('Imported [%s] rows from [%s]' % (num, args['<filename>']))
    elif args.get('query'):
        filters = {}
        if args.get('-f'):
            filters = dict(map(lambda x: tuple(x.split('=')),
                               args.get('-f', '').split(',')))
            # e.g., filters == dict(STB='stb1', REV='4.00')

        order_fields = []
        if args.get('-o'):
            order_fields = args['-o'].split(',')
            # e.g., order_fields == ['TITLE', 'DATE']

        select_fields = []
        if args.get('-s'):
            select_fields = args['-s'].split(',')
            # e.g., select_fields == ['TITLE:count', 'DATE']

        query(filters=filters,
              group_by=args.get('-g'),
              order_fields=order_fields,
              select_fields=select_fields,
              outfile_obj=outfile_obj)

if __name__ == '__main__':
    args = docopt.docopt(doc=__doc__, help=True)
    run(args)
