import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

JOB_NAME = 'user_subscription'
S3_BUCKET = 'hbo-dap-dcm'
S3_PREFIX = 'hbonow/subscription_market_state/parquet/'
OUTPUT_PATH = 's3://hbo-dap-research/dev_research_user/anchan/user_sub_length_test'

query_dt = '2018-10-15'


def get_next_month(dt):
    date = datetime.strptime(dt, '%Y-%m-%d')
    next_year = date.year + (date.month / 12)
    next_month = ((date.month % 12) + 1)

    try:
        return datetime(date.year + (date.month / 12), ((date.month % 12) + 1), date.day)

    except ValueError:
        if next_month in (4, 6, 9, 11):
            return datetime(next_year, next_month, 30)
        if next_month == 2:
            if next_year % 4 == 0:
                return datetime(next_year, next_month, 29)
            else:
                return datetime(next_year, next_month, 28)


def later_dt(dt):
    next_month = get_next_month(dt)
    return (next_month + timedelta(days=5)).strftime('%Y-%m-%d')


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('yarn') \
        .appName(JOB_NAME) \
        .getOrCreate()

    # need to get data from potentially 3 months
    # could not just use whole parquet folder because columns were different Q3 2018
    # this will change when we use a different subscription state table
    query_date = datetime.strptime(query_dt, '%Y-%m-%d')
    next_month_date = get_next_month(query_dt)
    churn_check_date = (next_month_date + timedelta(days=5))

    INPUT_PATHS = list(set([
        os.path.join('s3://', S3_BUCKET, S3_PREFIX, 'yr={}'.format(date.year), 'mo={:02d}'.format(date.month))
        for date in [query_date, next_month_date, churn_check_date]]))

    subs_1 = spark.read.load(INPUT_PATHS[0])
    subs_2 = spark.read.load(INPUT_PATHS[1])
    columns = list(set(subs_1.columns).intersection(subs_2.columns))
    all_subs = subs_1.select(columns).union(subs_2.select(columns))

    # might have to look in 3 months if end of month
    if len(INPUT_PATHS) > 2:
        subs_3 = spark.read.load(INPUT_PATHS[2])
        columns = list(set(all_subs.columns).intersection(subs_3.columns))
        all_subs = all_subs.select(columns).union(subs_3.select(columns))

    all_subs.createOrReplaceTempView("sub_table")

    sub_sql = """
    select
        query_date.user_id,

        case
            when last_reconnect_date is null then 0
            else 1
        end as has_disconnected,
        case
            when last_disconnect_date is null then 0
            else datediff(last_disconnect_date, first_purchase_date)
        end as previous_days_active,
        case
            when last_reconnect_date is null then
                datediff(to_date('{query_date}', 'yyyy-MM-dd'), first_purchase_date)
            else
               datediff(to_date('{query_date}', 'yyyy-MM-dd'), last_reconnect_date)
        end as current_days_active,
        case
            when last_reconnect_date is null then
                datediff(to_date('{query_date}', 'yyyy-MM-dd'), first_purchase_date)
            else
               datediff(last_disconnect_date, first_purchase_date)  + datediff(to_date('{query_date}', 'yyyy-MM-dd'), last_reconnect_date)
        end as total_days_active,

        -- user counts as churned if churned any time from expiration
        max(case when later_dates.current_subscription_state = 3 then 1 else 0 end) churned
    from
    (
        select
            external_user_id user_id,
            current_subscription_state,
            current_subscription_start_date,
            current_subscription_end_date,
            offer_type,
            location_zip,
            location_state,
            location_timezone,
            last_transaction_provider,
            first_purchase_date,
            last_conversion_date,
            last_reconnect_date,
            last_disconnect_date
        from
           sub_table
        where
          dt = '{query_date}'
          and current_subscription_end_date >=  '{query_date}'
          and current_subscription_state = 2
          and external_user_id is not null
    ) query_date

    inner join

    (
        select
          dt,
          external_user_id user_id,
          current_subscription_end_date,
          current_subscription_state
        from
           sub_table
        where
          dt between '{query_date}' and '{after_expiration}'
          and current_subscription_state in (2, 3)
          and external_user_id is not null

    ) later_dates

    on
        query_date.user_id = later_dates.user_id
    where
        later_dates.dt >= query_date.current_subscription_end_date
    group by
        query_date.user_id,
        case
            when last_reconnect_date is null then 0
            else 1
        end,
        case
            when last_disconnect_date is null then 0
            else datediff(last_disconnect_date, first_purchase_date)
        end,
        case
            when last_reconnect_date is null then
                datediff(to_date('{query_date}', 'yyyy-MM-dd'), first_purchase_date)
            else
               datediff(to_date('{query_date}', 'yyyy-MM-dd'), last_reconnect_date)
        end,
        case
            when last_reconnect_date is null then
                datediff(to_date('{query_date}', 'yyyy-MM-dd'), first_purchase_date)
            else
               datediff(last_disconnect_date, first_purchase_date)  + datediff(to_date('{query_date}', 'yyyy-MM-dd'), last_reconnect_date)
        end
    """.format(query_date=query_dt, after_expiration=churn_check_date.strftime('%Y-%m-%d'))

    subs = spark.sql(sub_sql)

    subs.write.parquet(OUTPUT_PATH, mode='overwrite')
