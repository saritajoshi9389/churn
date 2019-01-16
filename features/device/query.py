device_sql = """
select
    user_id,
    device_code,
    sum(session_length_min) device_minutes
from
(
    select
        user_id,
        device_code,
        session_id,
        cast(unix_timestamp(session_end) - unix_timestamp(session_start) as float) / 60.0 as session_length_min
    from
        sessions

) session_lengths
where
    session_length_min < 480
group by
    user_id,
    device_code
"""
