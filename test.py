from io import StringIO
import pytest
from datastore import import_data, _iter_import_record
from run_db import run


def _test_run(args, expect):
    # run with :args: and assert that :expect: equals the actual output
    actual_output = StringIO()
    run(args, outfile_obj=actual_output)
    assert actual_output.getvalue() == expect, (
        '\n\nexpected:\n%s\nactual:\n%s\n'
        % (expect, actual_output.getvalue()))


def test_import_and_queries():
    input_ = '''\
STB|TITLE|PROVIDER|DATE|REV|VIEW_TIME
stb1|the matrix|warner bros|2014-04-01|4.00|1:30
stb1|unbreakable|buena vista|2014-04-03|6.00|2:05
stb2|the hobbit|warner bros|2014-04-02|8.00|2:45
stb3|the matrix|warner bros|2014-04-02|4.00|1:05
'''

    import_data(infile_obj=StringIO(input_), delete_first=True)

    # select all the same fields as the input
    select_fields = input_.splitlines()[0].replace('|', ',')
    _test_run(args={'query': True, '-s': select_fields},
              expect=input_)

    # select only one field
    _test_run(args={'query': True, '-s': 'STB'},
              expect='''\
STB
stb1
stb1
stb2
stb3
''')

    # select all the fields by default (alphabetical order)
    _test_run(args={'query': True},
              expect='''\
DATE|PROVIDER|REV|STB|TITLE|VIEW_TIME
2014-04-01|warner bros|4.00|stb1|the matrix|1:30
2014-04-03|buena vista|6.00|stb1|unbreakable|2:05
2014-04-02|warner bros|8.00|stb2|the hobbit|2:45
2014-04-02|warner bros|4.00|stb3|the matrix|1:05
''')

    # filter with a single field, return 1 record
    _test_run(args={'query': True, '-f': 'REV=8.00'},
              expect='''\
DATE|PROVIDER|REV|STB|TITLE|VIEW_TIME
2014-04-02|warner bros|8.00|stb2|the hobbit|2:45
''')

    # filter with a single field, return 2 records
    _test_run(args={'query': True, '-f': 'REV=4.00'},
              expect='''\
DATE|PROVIDER|REV|STB|TITLE|VIEW_TIME
2014-04-01|warner bros|4.00|stb1|the matrix|1:30
2014-04-02|warner bros|4.00|stb3|the matrix|1:05
''')

    # filter with two fields, return 1 record
    _test_run(args={'query': True, '-f': 'REV=4.00,STB=stb3'},
              expect='''\
DATE|PROVIDER|REV|STB|TITLE|VIEW_TIME
2014-04-02|warner bros|4.00|stb3|the matrix|1:05
''')

    # filter to return 2 records with multiple sort field
    _test_run(args={'query': True,
                    '-s': 'TITLE,REV,DATE',
                    '-o': 'DATE,TITLE'},
              expect='''\
TITLE|REV|DATE
the matrix|4.00|2014-04-01
the hobbit|8.00|2014-04-02
the matrix|4.00|2014-04-02
unbreakable|6.00|2014-04-03
''')

    # group-by, no aggregates
    _test_run(args={'query': True,
                    '-s': 'TITLE',
                    '-g': 'TITLE',
                    '-o': 'TITLE'},
              expect='''\
TITLE
the hobbit
the matrix
unbreakable
''')

    # group-by, with sum
    _test_run(args={'query': True,
                    '-s': 'TITLE,REV:sum',
                    '-g': 'TITLE',
                    '-o': 'TITLE'},
              expect='''\
TITLE|REV:sum
the hobbit|8.00
the matrix|8.00
unbreakable|6.00
''')

    # group-by, with min
    _test_run(args={'query': True,
                    '-s': 'TITLE,DATE:min',
                    '-g': 'TITLE',
                    '-o': 'TITLE'},
              expect='''\
TITLE|DATE:min
the hobbit|2014-04-02
the matrix|2014-04-01
unbreakable|2014-04-03
''')

    # group-by, with filter, with max
    _test_run(args={'query': True,
                    '-s': 'TITLE,DATE:max',
                    '-f': 'TITLE=the matrix',
                    '-g': 'TITLE',
                    '-o': 'TITLE'},
              expect='''\
TITLE|DATE:max
the matrix|2014-04-02
''')

    # group-by, with count
    _test_run(args={'query': True,
                    '-s': 'TITLE:count',
                    '-g': 'TITLE',
                    '-o': 'TITLE'},
              expect='''\
TITLE|TITLE:count
the hobbit|1
the matrix|2
unbreakable|1
''')

    # group-by, with collect and sum
    _test_run(args={'query': True,
                    '-s': 'TITLE,REV:sum,STB:collect',
                    '-g': 'TITLE',
                    '-o': 'TITLE'},
              expect='''\
TITLE|REV:sum|STB:collect
the hobbit|8.00|[stb2]
the matrix|8.00|[stb1,stb3]
unbreakable|6.00|[stb1]
''')

    # group-by with aggregates for all fields
    _test_run(args={'query': True,
                    '-s': 'TITLE:count,REV:sum,STB:collect,PROVIDER:collect'
                          ',VIEW_TIME:sum,DATE:max',
                    '-g': 'TITLE',
                    '-o': 'TITLE'},
              expect='''\
TITLE|TITLE:count|REV:sum|STB:collect|PROVIDER:collect|VIEW_TIME:sum|DATE:max
the hobbit|1|8.00|[stb2]|[warner bros]|2:45|2014-04-02
the matrix|2|8.00|[stb1,stb3]|[warner bros]|2:35|2014-04-02
unbreakable|1|6.00|[stb1]|[buena vista]|2:05|2014-04-03
''')


def test_iter_import_record():
    input_ = '''\
STB|TITLE|PROVIDER|DATE|REV|VIEW_TIME
stb1|the matrix|warner bros|2014-04-01|4.00|1:30
stb1|unbreakable|buena vista|2014-04-03|6.00|2:05'''
    record = _iter_import_record(infile_obj=StringIO(input_))
    assert next(record) == dict(
        STB='stb1',
        TITLE='the matrix',
        PROVIDER='warner bros',
        DATE='2014-04-01',
        REV='4.00',
        VIEW_TIME='1:30')
    assert next(record) == dict(
        STB='stb1',
        TITLE='unbreakable',
        PROVIDER='buena vista',
        DATE='2014-04-03',
        REV='6.00',
        VIEW_TIME='2:05')
    with pytest.raises(StopIteration):
        assert next(record)
