from features import utils

user_cast_sql = """
    select
        viewerId,
        cast(`start time (unix time)` as int) start_time_unix_time,
        cast(`startup time (ms)` as int) startup_time_ms,
        cast(`playing time (ms)` as int) playing_time_ms,
        cast(`buffering time (ms)` as int) buffering_time_ms,
        cast(interrupts as int) interrupts,
        cast(`average bitrate (kbps)` as int) average_bitrate_kbps,
        cast(`connection induced re-buffering time (ms)` as int) as connection_induced_rebuffering_time_ms,
        cast(`video restart time (ms)` as int) as video_restart_time_ms,
        cast(`re-joined count` as int) as rejoined_count,
        cast(`startup error` as int) as startup_error,
        cast(VPF as int) as video_playback_failure,
        dt
    from
        conviva_strings
    """



def get_perf_sql(query_dt, days_back=28):
    # startup_time_ms = -1 means EBVS
    # average_bitrate_kbps = 0 means could not get bitrate
    valid_startup_time = "case when startup_time_ms > 0 then startup_time_ms else null end"
    valid_bitrate = "case when average_bitrate_kbps > 0 then average_bitrate_kbps else null end"

    # compute user aggregates
    return """
    select
        viewerid user_id,
        avg({startup_time}) avg_startup_time,
        approx_percentile({startup_time}, 0.5) median_startup_time,
        sum({startup_time}) total_startup_time,
        sum(startup_error) total_startup_errors,
        avg(interrupts) avg_interrupts,
        approx_percentile(interrupts, 0.5) median_interrupts,
        sum(interrupts) total_interrupts,
        avg(buffering_time_ms) avg_buffering_time,
        approx_percentile(buffering_time_ms, 0.5) median_buffering_time,
        sum(buffering_time_ms) total_buffering_time,
        avg({bitrate}) avg_bitrate,
        approx_percentile({bitrate}, 0.5) median_bitrate
    from
        conviva_streams
    where
        dt between '{dt_start}' and '{query_dt}'
        and from_unixtime(start_time_unix_time, 'yyyy-MM-dd') >= '{dt_start}'
        and from_unixtime(start_time_unix_time, 'yyyy-MM-dd') < '{query_dt}'
    group by
        viewerid
    """.format(startup_time=valid_startup_time,
               bitrate=valid_bitrate,
               dt_start=utils.dt_start(query_dt, days_back),
               query_dt=query_dt)
