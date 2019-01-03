def get_sub_sql(query_dt='', max_expiration_dt='', after_expiration_dt=''):
    return """
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
          first_purchase_date,
          last_conversion_date,
          last_reconnect_date,
          last_disconnect_date
      from
         sub_table
      where
        dt = '{query_date}'
        -- exclude those mysteriously active but expired
        and date_format(current_subscription_end_date, 'yyyy-MM-dd') >= '{query_date}'
        -- only with one-month expiration terms
        and date_format(current_subscription_end_date, 'yyyy-MM-dd') <= '{max_expiration_date}'
        and current_subscription_state = 2
        and external_user_id is not null
    ) query_date

    inner join

    (
      select
        dt,
        external_user_id user_id,
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
      later_dates.dt >= date_format(query_date.current_subscription_end_date, 'yyyy-MM-dd')
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
    """.format(query_date=query_dt,
               max_expiration_date=max_expiration_dt,
               after_expiration=after_expiration_dt)
