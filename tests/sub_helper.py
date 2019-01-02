query_dt_records = [
    {
        'external_user_id': 1,
        'current_subscription_state': 2,
        'current_subscription_start_date': '2018-10-01 20:00:00',
        'current_subscription_end_date': '2018-11-01 20:00:00',
        'first_purchase_date': '2018-07-01 20:00:00',
        'last_conversion_date': '2018-08-01 20:00:00',
        'last_reconnect_date': None,
        'last_disconnect_date': None
    },
    {
        'external_user_id': 2,
        'current_subscription_state': 2,
        'current_subscription_start_date': '2018-10-13 20:00:00',
        'current_subscription_end_date': '2018-11-13 20:00:00',
        'first_purchase_date': '2017-07-13 20:00:00',
        'last_conversion_date': '2017-08-13 20:00:00',
        'last_reconnect_date': None,
        'last_disconnect_date': None
    },
    # free trial user
    {
        'external_user_id': 3,
        'current_subscription_state': 1,
        'current_subscription_start_date': '2018-10-01 20:00:00',
        'current_subscription_end_date': '2018-11-01 20:00:00',
        'first_purchase_date': '2018-10-01 20:00:00',
        'last_conversion_date': None,
        'last_reconnect_date': None,
        'last_disconnect_date': None
    },
    # churned user
    {
        'external_user_id': 4,
        'current_subscription_state': 3,
        'current_subscription_start_date': '2016-10-01 20:00:00',
        'current_subscription_end_date': '2016-11-01 20:00:00',
        'first_purchase_date': '2016-08-01 20:00:00',
        'last_conversion_date': '2016-09-01 20:00:00',
        'last_reconnect_date': None,
        'last_disconnect_date': None
    },
    # user who has disconnected and reconnected
    {
        'external_user_id': 5,
        'current_subscription_state': 2,
        'current_subscription_start_date': '2018-10-01 20:00:00',
        'current_subscription_end_date': '2018-11-01 20:00:00',
        'first_purchase_date': '2017-07-01 20:00:00',
        'last_conversion_date': '2017-08-01 20:00:00',
        'last_reconnect_date': '2018-07-01 20:00:00',
        'last_disconnect_date': '2017-10-01 20:00:00'
    }
]

later_dt_records = [
    {
        'external_user_id': 1,
        'current_subscription_state': 2,
        'current_subscription_start_date': '2018-11-01 20:00:00',
        'current_subscription_end_date': '2018-12-01 20:00:00',
        'first_purchase_date': '2018-07-01 20:00:00',
        'last_conversion_date': '2018-08-01 20:00:00',
        'last_reconnect_date': None,
        'last_disconnect_date': None
    },
    {
        'external_user_id': 2,
        'current_subscription_state': 3,
        'current_subscription_start_date': '2018-10-13 20:00:00',
        'current_subscription_end_date': '2018-11-13 20:00:00',
        'first_purchase_date': '2017-07-13 20:00:00',
        'last_conversion_date': '2017-08-13 20:00:00',
        'last_reconnect_date': None,
        'last_disconnect_date': None
    },
    {
        'external_user_id': 3,
        'current_subscription_state': 2,
        'current_subscription_start_date': '2018-11-01 20:00:00',
        'current_subscription_end_date': '2018-12-01 20:00:00',
        'first_purchase_date': '2018-10-01 20:00:00',
        'last_conversion_date': '2018-11-01 20:00:00',
        'last_reconnect_date': None,
        'last_disconnect_date': None
    },
    {
        'external_user_id': 5,
        'current_subscription_state': 2,
        'current_subscription_start_date': '2018-11-01 20:00:00',
        'current_subscription_end_date': '2018-12-01 20:00:00',
        'first_purchase_date': '2017-07-01 20:00:00',
        'last_conversion_date': '2017-08-01 20:00:00',
        'last_reconnect_date': '2018-07-01 20:00:00',
        'last_disconnect_date': '2017-10-01 20:00:00'
    }
]