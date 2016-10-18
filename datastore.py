import csv
from glob import iglob
import os
import shutil

DATASTORE_DIR = './data'
INPUT_DELIMITER = '|'
STORE_DELIMITER = INPUT_DELIMITER
HEADERS_FILE = '%s/headers.csv' % DATASTORE_DIR


def get_datastore_headers():
    """Get the headers associated with records stored in the datastore."""
    return next(csv.reader(open(HEADERS_FILE, 'r'), delimiter=STORE_DELIMITER))


def import_data(infile_obj, delete_first=False):
    """Import csv data from the :infile_obj: file object into the
    datastore.
    """

    if delete_first and os.path.exists(DATASTORE_DIR):
        shutil.rmtree(DATASTORE_DIR)
    for num, rec in enumerate(_iter_import_record(infile_obj)):
        _save_record(rec)
    with open(HEADERS_FILE, 'w') as fh:
        fh.write(STORE_DELIMITER.join(sorted(rec.keys())))
    return num + 1


def iter_filtered_recs(filters={}):
    """Generator to get the next filtered record from the datastore,
    with :filters: applied.
    """

    # filter by the dirname or filename if applicable
    filters = filters.copy()
    date = filters.pop('DATE', '*')
    stb = filters.pop('STB', '*')
    title = filters.pop('TITLE', '*')

    path = _get_path(date=date, stb=stb, title=title)
    headers = get_datastore_headers()

    for filename in iglob(path):
        with open(filename) as fh:
            values = next(csv.reader(fh, delimiter=STORE_DELIMITER))
            rec = dict(zip(headers, values))
            if all([val == rec[key] for key, val in filters.items()]):
                yield rec


def _get_path(date, stb, title, make_dirs=False):
    path = '{datastore}/{stb}/{date}'.format(datastore=DATASTORE_DIR,
                                             stb=stb, date=date)
    if make_dirs:
        os.makedirs(path, exist_ok=True)
    return '%s/%s' % (path, title)


def _iter_import_record(infile_obj, delimiter=INPUT_DELIMITER):
    """Generator to get the next row from the {infile_obj} file object."""
    rows = csv.reader(infile_obj, delimiter=delimiter)
    headers = next(rows)
    for row in rows:
        rec = dict(zip(headers, row))
        yield rec


def _save_record(record):
    path = _get_path(date=record['DATE'],
                     stb=record['STB'],
                     title=record['TITLE'],
                     make_dirs=True)
    with open(path, 'w') as fh:
        fh.write(STORE_DELIMITER.join([str(value)
                                      for key, value in
                                      sorted(record.items())]))
