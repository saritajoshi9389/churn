activity_sql = """
select
    user_id,
    -- 1 day is counted for a session bridging 2 days
    count(distinct session_start_date) active_days,
    count(distinct session_id) num_sessions,
    avg(session_length_min) avg_session_length,
    percentile_approx(session_length_min, 0.5) median_session_length,
    sum(session_length_min) total_timespent
from
(
    select
        user_id,
        session_id,
        cast(session_start as date) session_start_date,
        cast(unix_timestamp(session_end) - unix_timestamp(session_start) as float) / 60.0 as session_length_min
    from
        sessions

) session_lengths
where
    -- remove sessions 8 hours or longer
    session_length_min < 480
group by
    user_id

"""
