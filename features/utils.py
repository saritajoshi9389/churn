from datetime import datetime, timedelta


def dt_start(dt, days_back=28):
    date_format = '%Y-%m-%d'
    date = datetime.strptime(dt, date_format)
    date_start = date - timedelta(days_back)
    return date_start.strftime(date_format)
