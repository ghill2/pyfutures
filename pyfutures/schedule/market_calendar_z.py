assert list(data.keys()) == list(range(7))
        assert all(len(times) > 0 for times in data.values())

        # parse weekly calendar

        columns = ["open_time", "break_start_time", "break_end_time", "close_time"]
        calendar = pd.DataFrame(columns=columns, dtype=object)

        for dayofweek, time_pairs in data.items():

            closed_all_day = time_pairs == [(datetime.time(0, 0), datetime.time(0, 0))]
            no_break = len(time_pairs) == 1
            has_break = len(time_pairs) == 2
            if closed_all_day:
                calendar.loc[len(calendar)] = (
                    None,
                    None,
                    None,
                    None,
                )
            elif no_break:
                calendar.loc[len(calendar)] = (
                    time_pairs[0][0],
                    None,
                    None,
                    time_pairs[0][1],
                )
            elif has_break:
                calendar.loc[len(calendar)] = (
                    time_pairs[0][0],
                    time_pairs[0][1],
                    time_pairs[1][0],
                    time_pairs[1][1],
                )
            else:
                print(time_pairs)
                raise RuntimeError("Only one break per day supported")

        assert len(calendar) == 7

        # parse schedule
        start = pd.Timestamp("19800101")
        end = pd.Timestamp("20250101")
        df = pd.DataFrame(
            index=pd.date_range(start, end, freq='D', tz=self._timezone),
            columns=columns,
            data=None,
            dtype=object,
        )

        for dayofweek in range(7):

            mask = df.index.dayofweek == dayofweek

            times = calendar.loc[dayofweek].to_dict()

            for key, time in times.items():

                if time is None:
                    df[key][mask] = None
                    continue

                df[key][mask] = df.index[mask].copy() \
                    + pd.Timedelta(hours=time.hour, minutes=time.minute)

        self._schedule = df
