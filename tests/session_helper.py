dt_1_records = [
    {
        'properties': {
            'userId': 1,
            'sessionId': 'a1',
            'deviceCode': 'android',
            'eventTimestamp': '2018-12-04T20:00:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 1,
            'sessionId': 'a1',
            'deviceCode': 'android',
            'eventTimestamp': '2018-12-04T21:00:00.000Z'
        }
    },
    # sessions in wrong dt partition don't count
    {
        'properties': {
            'userId': 1,
            'sessionId': 'a1',
            'deviceCode': 'android',
            'eventTimestamp': '2018-12-0323:59:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 1,
            'sessionId': 'a5',
            'deviceCode': 'android',
            'eventTimestamp': '2018-12-04T09:00:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 1,
            'sessionId': 'a5',
            'deviceCode': 'android',
            'eventTimestamp': '2018-12-04T10:00:00.000Z'
        }
    },
    {
        'properties': {

            'userId': 2,
            'sessionId': 'b1',
            'deviceCode': 'appletv',
            'eventTimestamp': '2018-12-04T09:00:00.000Z'
        }
    },
    {
        'properties': {

            'userId': 2,
            'sessionId': 'b1',
            'deviceCode': 'appletv',
            'eventTimestamp': '2018-12-04T09:05:00.000Z'
        }
    },
    {
        'properties': {

            'userId': 2,
            'sessionId': 'b1',
            'deviceCode': 'appletv',
            'eventTimestamp': '2018-12-04T10:00:00.000Z'
        }
    }
]

dt_2_records = [
    {
        'properties': {
            'userId': 1,
            'sessionId': 'a2',
            'deviceCode': 'desktop',
            'eventTimestamp': '2018-12-15T20:00:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 1,
            'sessionId': 'a2',
            'deviceCode': 'desktop',
            'eventTimestamp': '2018-12-15T22:00:00.000Z'
        }
    }
]

dt_3_records = [
    {
        'properties': {
            'userId': 1,
            'sessionId': 'a4',
            'deviceCode': 'firetv',
            'eventTimestamp': '2019-01-01T05:00:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 1,
            'sessionId': 'a3',
            'deviceCode': 'firetv',
            'eventTimestamp': '2018-12-31T21:00:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 1,
            'sessionId': 'a3',
            'deviceCode': 'firetv',
            'eventTimestamp': '2018-12-31T23:00:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 2,
            'sessionId': 'b3',
            'deviceCode': 'appletv',
            'eventTimestamp': '2018-12-31T09:00:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 2,
            'sessionId': 'b3',
            'deviceCode': 'appletv',
            'eventTimestamp': '2018-12-31T09:05:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 2,
            'sessionId': 'b3',
            'deviceCode': 'appletv',
            'eventTimestamp': '2018-12-31T10:00:00.000Z'
        }
    },
    # overlong session will be in sessions but not dependent queries
    {
        'properties': {
            'userId': 2,
            'sessionId': 'b4',
            'deviceCode': 'appletv',
            'eventTimestamp': '2018-12-31T12:00:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 2,
            'sessionId': 'b4',
            'deviceCode': 'appletv',
            'eventTimestamp': '2018-12-31T21:00:00.000Z'
        }
    },
    {
        'properties': {
            'userId': 2,
            'sessionId': 'b5',
            'deviceCode': 'appletv',
            'eventTimestamp': '2019-01-01T10:00:00.000Z'
        }
    }
]