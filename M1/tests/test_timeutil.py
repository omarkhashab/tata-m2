import pandas as pd

from kestrel_m1 import timeutil as tu


def ts(s):
    return pd.Timestamp(s, tz="UTC")


def test_activity_time_formats_all_parse_to_the_same_utc_instant():
    raw = pd.Series(["2026-03-14T09:22:01.412Z", "1773480121412", "14/03/2026 09:22:01", "2026-03-14T11:22:01.412+02:00"])
    p = tu.parse_activity_time(raw)
    assert list(p.fmt) == ["iso8601", "epoch_ms", "dmy_hms", "iso8601"]
    assert p.ts[0] == p.ts[1] == p.ts[3] == ts("2026-03-14 09:22:01.412")
    assert p.ts[2] == ts("2026-03-14 09:22:01")           # dmy carries no milliseconds
    assert list(p.nonstandard) == [False, True, True, False]


def test_impossible_and_garbage_times_are_nat_not_guessed():
    p = tu.parse_activity_time(pd.Series(["2026-02-30T10:00:00Z", "not-a-time", None, "31/02/2026 10:00:00"]))
    assert p.ts.isna().all()
    assert list(p.fmt) == ["iso8601", "unknown", "null", "dmy_hms"]
    assert not p.nonstandard.any()


def test_dmy_is_day_first():
    assert tu.parse_activity_time(pd.Series(["02/03/2026 10:00:00"])).ts[0] == ts("2026-03-02 10:00:00")


def test_iso_offsets_are_converted_not_dropped():
    out = tu.parse_iso_utc(pd.Series(["2026-01-31T04:19:25+03:00", "2026-01-31T04:19:25-05:00", "2026-01-31T04:19:25"]))
    assert out[0] == ts("2026-01-31 01:19:25")
    assert out[1] == ts("2026-01-31 09:19:25")
    assert pd.isna(out[2])                                  # naive ISO is not a valid canonical timestamp


def test_audit_local_time_gets_year_and_offset():
    out = tu.audit_local_to_utc(pd.Series(["Apr  1 01:10:00", "Mar 14 11:22:04"]))
    assert out[0] == ts("2026-03-31 23:10:00")
    assert out[1] == ts("2026-03-14 09:22:04")


def test_audit_per_host_skew_is_applied_with_fallback():
    log = pd.Series(["Mar 14 11:22:04", "Mar 14 11:22:04"])
    host = pd.Series(["sbx-a", "sbx-unknown"])
    out = tu.audit_local_to_utc(log, host, {"sbx-a": 7200 - 60})   # host clock 60 s slow
    assert out[0] == ts("2026-03-14 09:23:04")
    assert out[1] == ts("2026-03-14 09:22:04")


def test_lateness_and_late_flag():
    ing = pd.Series([ts("2026-03-14 09:00:11"), ts("2026-03-14 09:00:10"), pd.NaT])
    ev = pd.Series([ts("2026-03-14 09:00:00")] * 3)
    lat = tu.lateness_seconds(ing, ev)
    assert list(tu.is_late(lat)) == [True, False, False]   # strictly greater than 10 s
