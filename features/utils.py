import os
from datetime import datetime, timedelta


def dt_start(dt, days_back=28):
    date_format = '%Y-%m-%d'
    date = datetime.strptime(dt, date_format)
    date_start = date - timedelta(days_back)
    return date_start.strftime(date_format)


def dt_path(output_path, query_dt):
    return os.path.join(output_path, 'dt={}'.format(query_dt))


def yesterday_dt():
    return (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
