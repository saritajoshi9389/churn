from features import utils


def test_dt_start_default():
    test_dt = '2019-01-03'
    assert utils.dt_start(test_dt) == '2018-12-06'


def test_dt_start():
    test_dt = '2019-01-03'
    assert utils.dt_start(test_dt, 7) == '2018-12-27'


def test_dt_path():
    assert utils.dt_path('s3://mypath', '2019-01-09') == 's3://mypath/dt=2019-01-09'
