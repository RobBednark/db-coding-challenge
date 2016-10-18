from decimal import Decimal
from operator import itemgetter
import sys
from datastore import get_datastore_headers, iter_filtered_recs

OUTPUT_DELIMITER = '|'


def query(order_fields=[], filters={}, select_fields=[],
          group_by='', outfile_obj=sys.stdout):
    """query for records and write the results to :outfile_obj:
    :param order_fields: list of fieldnames to order by, primary first
    :param filters: dict of filters to apply, where key is the
           field_name, and value is the value to match
           e.g., filters={'STB': 'stb1', 'REV': '4.00')
    :param select_fields: list of field names to select to be output,
           with an optional ':aggregate_function' suffix on each
           field name (must specify group_by as
           well).  aggregate_function must be one of
           (min, max, count, sum, collect).
           e.g., select_fields == ['TITLE:count', 'DATE']
    :param group_by: a field name (string) to group-by
           e.g., group_by='TITLE'
    :param outfile_obj: a file object where the output will be written
           to
    """

    recs = list(iter_filtered_recs(filters))
    if not select_fields:
        # no fields specified, so show all of them
        select_fields = get_datastore_headers()
    if group_by:
        _apply_group_by(recs, group_by, select_fields)
    if order_fields:
        recs.sort(key=itemgetter(*order_fields))

    new_select_fields = []
    for field in select_fields:
        if field.find(':') >= 0:
            orig_field, agg = field.split(':')
            if agg == 'count':
                # for counts, show both the original field and the count field
                new_select_fields.append(orig_field)
        new_select_fields.append(field)
    select_fields = new_select_fields

    print(OUTPUT_DELIMITER.join(select_fields), file=outfile_obj)

    for rec in recs:
        if rec.get('_ignore'):
            continue
        print(OUTPUT_DELIMITER.join([_format(field, rec[field])
                                     for field in select_fields]),
              file=outfile_obj)


def _apply_aggregates(agg_rec, agg_fields, rec=None):
    """aggregate the :agg_fields: values in :rec: and :agg_rec: and put
    the result in :agg_rec:
    If :rec: is None, then :agg_rec: will have it's aggregate fields
    converted to the correct data types.
    """

    for agg_field in agg_fields:
        orig_field, oper = agg_field.split(':')
        if rec is None:
            # first record, so convert the fields to the correct data types
            if oper in ['max', 'min', 'sum']:
                value = _convert_for_cmp(orig_field, agg_rec[orig_field])
            elif oper == 'collect':
                value = {agg_rec[orig_field]}
            elif oper == 'count':
                value = 1
        else:
            # assert: not first record, so agg_rec has correct data type
            if oper == 'max':
                value = max(_convert_for_cmp(orig_field, rec[orig_field]),
                            agg_rec[agg_field])
            elif oper == 'min':
                value = min(_convert_for_cmp(orig_field, rec[orig_field]),
                            agg_rec[agg_field])
            elif oper == 'sum':
                value = (_convert_for_cmp(orig_field, rec[orig_field]) +
                         agg_rec[agg_field])
            elif oper == 'collect':
                value = {rec[orig_field]} | agg_rec[agg_field]
            elif oper == 'count':
                value = agg_rec[agg_field] + 1
        agg_rec[agg_field] = value


def _apply_group_by(recs, group_by, select_fields):
    """Apply the group-by operation to :recs: in-place, and aggregate
    any values specified in :select_fields: via the
    ':aggregate_function' suffixes.
    :param recs: The records (list of dicts) to apply the group_by to.
                 They will be modified in place, with the first record
                 in a group containing the aggregate values, and the
                 subsequent records in the group will have
                 rec['_ignore']==True
    :param group_by: the field name (string) to group by
    :param select_fields: list of field names to select to be output,
           with an optional ':aggregate_function' suffix on each field
           name.
           e.g., select_fields == ['TITLE:count', 'DATE']
    """
    group = {}
    agg_fields = [f for f in select_fields if f.find(':') >= 0]
    for num, rec in enumerate(recs):
        group_by_val = rec[group_by]
        if group_by_val in group:
            if agg_fields:
                _apply_aggregates(rec=rec, agg_rec=group[group_by_val],
                                  agg_fields=agg_fields)
            # Ignore this rec, because we will only use the first grouped
            # rec seen and update that first one with the aggregate values.
            rec['_ignore'] = True
        else:
            # the first group_by_val seen, so save it to group
            group[group_by_val] = rec
            if agg_fields:
                # make this the agg rec
                _apply_aggregates(agg_rec=rec, agg_fields=agg_fields)


def _convert_for_cmp(field_name, value):
    """Convert fields that need to be numerically compared."""

    if field_name == 'REV':
        return Decimal(value)
    elif field_name == 'VIEW_TIME':
        hrs, mins = value.split(':')
        return (int(hrs) * 60) + int(mins)
    else:
        return value


def _format(field, value):
    """Format :collect fields and VIEW_TIME: fields for output."""
    if field.find(':collect') >= 0:
        converted = '['
        for elem in sorted(value):
            converted += elem + ','
        return converted[:-1] + ']'
    elif ((field.find('VIEW_TIME:') >= 0)):
        return '{hrs}:{mins:02}'.format(hrs=(int(value/60)),
                                        mins=(value % 60))
    else:
        return str(value)
