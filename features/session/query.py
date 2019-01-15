from features import utils


def get_session_sql(query_dt='', days_back=1):
    return """
    select
        user_id,
        session_id,
        case when device_code = 'taco' then 'appletv' else device_code end as device_code,
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
        case when device_code = 'taco' then 'appletv' else device_code end
    """.format(dt_start=utils.dt_start(query_dt, days_back),
               query_dt=query_dt)
