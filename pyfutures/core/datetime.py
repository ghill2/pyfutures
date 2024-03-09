import pandas as pd


# UNIX_EPOCH = pd.Timestamp("1970-01-01", tz="UTC")


MILLISECONDS_IN_SECOND = 1_000
MICROSECONDS_IN_SECOND = 1_000_000
NANOSECONDS_IN_SECOND = 1_000_000_000
NANOSECONDS_IN_MILLISECOND = 1_000_000
NANOSECONDS_IN_MICROSECOND = 1_000
NANOSECONDS_IN_DAY = 86400 * NANOSECONDS_IN_SECOND


def timedelta_to_nanos(td):
    """
    The maximum resolution of a Python `timedelta` is 1 microsecond (μs).
    """
    return (
        td.days * NANOSECONDS_IN_DAY + td.seconds * NANOSECONDS_IN_SECOND
        # + td.microsecond * NANOSECONDS_IN_MICROSECOND
    )


def dt_to_unix_nanos_vectorized(datetimes: pd.Series):
    UNIX_EPOCH = pd.Timestamp("1970-01-01", tz="UTC")
    return (datetimes - UNIX_EPOCH).view("int64").astype("uint64")


def unix_nanos_to_dt_vectorized(values: pd.Series):
    return pd.to_datetime(values, unit="ns", utc=True)
