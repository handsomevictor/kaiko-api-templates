import datetime


def time_convert(timestamp):
    if type(timestamp) == datetime.datetime or type(timestamp) == datetime.date:
        start_time_str = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_time_dt = timestamp
    else:
        start_time_str = timestamp
        start_time_dt = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
    return start_time_str, start_time_dt
