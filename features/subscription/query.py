def get_sub_sql(query_dt='', month_later_dt=''):
    return """
    select
      query_date.user_id,
      case when subsequent_churn.user_id is null then 0 else 1 end as churned,
      purchase_n as months_subscribing,
      has_disconnected
    from
    (
      select
          user_id,
          date_format(subscription_expire_date, 'yyyy-MM-dd') expire_dt,
          purchase_n,
          case when months_between(subscription_start_date, first_purchase_date) - purchase_n > 3 then 1 else 0 end has_disconnected
      from
         sub_table
      where
        dt = '{query_date}'
        and subscription_type = 'monthly paying'
        and subscription_term < 35
        and subscription_active = 1
    ) query_date

    left outer join

    (
      select
        user_id,
        dt
      from
        sub_table
      where
        dt between '{query_date}' and '{month_later}'
        and subscription_active = 1
        and subscription_drop = 1

    ) subsequent_churn

    on
      query_date.user_id = subsequent_churn.user_id
      and query_date.expire_dt = subsequent_churn.dt
    """.format(query_date=query_dt,
               month_later=month_later_dt)
