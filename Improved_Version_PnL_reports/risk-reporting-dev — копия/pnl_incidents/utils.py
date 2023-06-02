from sys import argv
import datetime
import re
from datetime import datetime, timedelta


class Params:
    @staticmethod
    def get_param(
            param_name: str,
            assertion_error_text: str = 'Missing required parameter',
            single: bool = True,
            required: bool = True,
            value_if_missing=None
    ):
        param = f'-{param_name}'

        if required:
            assert param in argv, assertion_error_text

        if param not in argv:
            return value_if_missing

        if single:
            return argv[argv.index(param) + 1]

        params = []
        for i in range(argv.index(param) + 1, len(argv)):
            param_val = argv[i]
            if re.compile(r'^-').match(param_val) is not None:
                break
            params.append(param_val)
        return params


def get_period_utc(tz_diff: int):
    date = Params.get_param('date', required=False)
    if date is not None:
        assert re.compile(r'^\d\d\d\d-\d\d-\d\d$').match(date) is not None, 'Wrong date format'
        start_utc = datetime(*[i for i in map(int, date.split('-'))]) - timedelta(hours=tz_diff)
        return start_utc, start_utc + timedelta(days=1) - timedelta(seconds=1)

    start = Params.get_param('from', required=False, single=False)
    end = Params.get_param('to', required=False, single=False)
    if start is not None and end is not None:
        assert len(start) > 0 and len(end) > 0, 'Input both -from and -to parameters'
        assert re.compile(r'^\d\d\d\d-\d\d-\d\d$').match(start[0]) is not None, 'Wrong date format (-from)'
        assert re.compile(r'^\d\d\d\d-\d\d-\d\d$').match(end[0]) is not None, 'Wrong date format (-to)'
        start_date, end_date = start[0].split('-'), end[0].split('-')
        if len(start) == 1 and len(end) == 1:
            start_time, end_time = [], ['23', '59', '59']
        else:
            assert re.compile(r'^\d\d:\d\d:\d\d$').match(start[1]) is not None, 'Wrong time format (-from)'
            assert re.compile(r'^\d\d:\d\d:\d\d$').match(end[1]) is not None, 'Wrong time format (-to)'
            start_time, end_time = start[1].split(':'), end[1].split(':')
        return datetime(*[i for i in map(int, start_date + start_time)]) - timedelta(hours=tz_diff), \
               datetime(*[i for i in map(int, end_date + end_time)]) - timedelta(hours=tz_diff)

    now = datetime.now().replace(microsecond=0)
    return now - timedelta(days=1, hours=tz_diff), now - timedelta(hours=tz_diff)
