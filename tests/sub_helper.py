query_dt_records = [
    {
        'user_id': 1,
        'subscription_start_date': '2018-10-01 20:00:00',
        'subscription_expire_date': '2018-11-01 20:00:00',
        'first_purchase_date': '2018-07-01 20:00:00',
        'purchase_n': 4,
        'subscription_active': 1,
        'subscription_drop': 0,
        'subscription_type': 'monthly paying',
        'subscription_term': 31
    },
    {
        'user_id': 2,
        'subscription_start_date': '2018-10-15 20:00:00',
        'subscription_expire_date': '2018-11-15 20:00:00',
        'first_purchase_date': '2017-07-15 20:00:00',
        'purchase_n': 4,
        'subscription_active': 1,
        'subscription_drop': 0,
        'subscription_type': 'monthly paying',
        'subscription_term': 31
    },
    # free trial user
    {
        'user_id': 3,
        'subscription_start_date': '2018-10-01 20:00:00',
        'subscription_expire_date': '2018-11-01 20:00:00',
        'first_purchase_date': '2018-10-01 20:00:00',
        'purchase_n': 1,
        'subscription_active': 1,
        'subscription_drop': 0,
        'subscription_type': '1-month free',
        'subscription_term': 31
    },
    # churned user
    {
        'user_id': 4,
        'subscription_start_date': '2016-10-01 20:00:00',
        'subscription_expire_date': '2016-11-01 20:00:00',
        'first_purchase_date': '2016-08-01 20:00:00',
        'purchase_n': 3,
        'subscription_active': 0,
        'subscription_drop': 0,
        'subscription_type': 'monthly paying',
        'subscription_term': 31
    },
    # user who has disconnected and reconnected
    {
        'user_id': 5,
        'subscription_start_date': '2018-10-01 20:00:00',
        'subscription_expire_date': '2018-11-01 20:00:00',
        'first_purchase_date': '2017-07-01 20:00:00',
        'purchase_n': 6,
        'subscription_active': 1,
        'subscription_drop': 0,
        'subscription_type': 'monthly paying',
        'subscription_term': 31
    }
]

later_dt_records = [
    {
        'user_id': 1,
        'subscription_start_date': '2018-11-01 20:00:00',
        'subscription_expire_date': '2018-12-01 20:00:00',
        'first_purchase_date': '2018-07-01 20:00:00',
        'purchase_n': 5,
        'subscription_active': 1,
        'subscription_drop': 0,
        'subscription_type': 'monthly paying',
        'subscription_term': 30
    },
    {
        'user_id': 2,
        'subscription_start_date': '2018-10-15 20:00:00',
        'subscription_expire_date': '2018-11-15 20:00:00',
        'first_purchase_date': '2017-07-15 20:00:00',
        'purchase_n': 4,
        'subscription_active': 1,
        'subscription_drop': 1,
        'subscription_type': 'monthly paying',
        'subscription_term': 31
    },
    {
        'user_id': 3,
        'subscription_start_date': '2018-11-01 20:00:00',
        'subscription_expire_date': '2018-12-01 20:00:00',
        'first_purchase_date': '2018-10-01 20:00:00',
        'purchase_n': 2,
        'subscription_active': 1,
        'subscription_drop': 0,
        'subscription_type': 'monthly paying',
        'subscription_term': 30
    },
    {
        'user_id': 5,
        'subscription_start_date': '2018-11-01 20:00:00',
        'subscription_expire_date': '2018-12-01 20:00:00',
        'first_purchase_date': '2017-07-01 20:00:00',
        'purchase_n': 7,
        'subscription_active': 1,
        'subscription_drop': 0,
        'subscription_type': 'monthly paying',
        'subscription_term': 30
    }
]