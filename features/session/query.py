from features import utils


def get_session_sql(query_dt='', days_back=28):
    return """
    select
        user_id,
        session_id,
        device,
        min(cast(event_timestamp as timestamp)) session_start,
        max(cast(event_timestamp as timestamp)) session_end
    from
        events
    where
        dt between '{dt_start}' and '{query_dt}'
        and substr(event_timestamp, 1, 10) >= '{dt_start}'
        and substr(event_timestamp, 1, 10) < '{query_dt}'
    group by
        user_id,
        session_id,
        device
    """.format(dt_start=utils.dt_start(query_dt, days_back),
               query_dt=query_dt)


activity_sql = """
select
    user_id,
    -- 1 day is counted for a session bridging 2 days
    count(distinct cast(session_start as date)) active_days,
    count(distinct session_id) num_sessions,
    avg(unix_timestamp(session_end) - unix_timestamp(session_start)) / 60.0 avg_session_length,
    cast(approx_percentile(unix_timestamp(session_end) - unix_timestamp(session_start), 0.5) as float) / 60.0 median_session_length,
    sum(unix_timestamp(session_end) - unix_timestamp(session_start)) / 60.0 total_timespent
from
    sessions
group by
    user_id
"""

device_sql = """
select
    user_id,
    device,
    sum(unix_timestamp(session_end) - unix_timestamp(session_start)) / 60.0 device_minutes
from
    sessions
group by
    user_id,
    device
"""
