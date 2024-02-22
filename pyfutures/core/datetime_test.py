import os
import random
import sys
from datetime import datetime

import pandas as pd
import pytest
from nautilus_trader.core.datetime import dt_to_unix_nanos
from pytower.core.datetime import dt_to_unix_nanos_vectorized


UNIX_EPOCH = pd.Timestamp("1970-01-01", tz="UTC")
NOW = pd.Timestamp(datetime.now(), tz="UTC")
random.seed(30)
# dt = random.randint(
#     UNIX_EPOCH,
#     datetime.now()
# )


def _generate_seed_datetimes(seed) -> pd.Series:
    size = 1000
    datetimes: pd.Series = (
        pd.date_range(
            start=UNIX_EPOCH,
            end=NOW,
        )
        .to_series()
        .sample(n=size, random_state=seed, ignore_index=True)
    )

    return datetimes


@pytest.mark.parametrize(
    "datetimes",
    [
        _generate_seed_datetimes(9874),
        _generate_seed_datetimes(12078),
        _generate_seed_datetimes(1936),
        _generate_seed_datetimes(20892),
        _generate_seed_datetimes(10892),
    ],
)
def test_vectorized_same_as_scalar_dt_to_unix_nanos(datetimes):
    expected = pd.Series(
        [dt_to_unix_nanos(dt) for dt in datetimes],
    )
    vectorized_result = dt_to_unix_nanos_vectorized(datetimes)
    assert (expected == vectorized_result).all()


if __name__ == "__main__":
    # test_vectorized_same_as_scalar(_generate_seed_datetimes(9874));
    sys.exit(
        pytest.main(
            [
                f"{os.path.abspath(__file__)}::test_vectorized_same_as_scalar_dt_to_unix_nanos",
                "-x",
                "-q",
            ],
        ),
    )
