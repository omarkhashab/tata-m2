"""SIEM alert <-> network flow correlation.

The targeted systems never saw a Kestrel session id, so an alert is linked to a flow
by what both sides did see (Dataset Description 3.8):

    alert.source_ip = flow.src_ip
    alert.destination_host = flow.dst_ip   and   alert.destination_port = flow.dst_port
    |alert.detected_at_utc - flow.flow_time_utc| <= N

N = 60 s (``DEFAULT_N_S``). Justification (``sensitivity``): matches grow until 45 s,
the detection lag of 97% of on-platform alerts (they trail their flow by 0-45 s), and
the counts are identical for every N from 45 s to 120 s; 60 s sits inside that
plateau with a margin. 67 further on-platform alerts trail a single key-matching flow
by 155-393 s. They are delayed detections, not coincidences: none of the 2,436 alerts
matched within 60 s has another key-matching flow 60-400 s away. A second plateau at
N = 394-500 s would add them with no new ambiguity (2,476 matched / 27 ambiguous /
1,251 unmatched, the unmatched being exactly the off-platform cloud-ids alerts). We
declare the tighter N = 60 s and report those 67 as unmatched; at 3600 s ambiguity
doubles.

Each alert gets exactly one status:

* ``matched``: exactly one candidate flow
* ``ambiguous``: two or more candidate flows. The flow is not chosen, but when every
  candidate belongs to the same session, the alert's *session* is still certain and is
  recorded (``session_resolution = 'single_session'``)
* ``unmatched``: no candidate. Never forced: e.g. cloud-ids ``*_external`` alerts come
  from source IPs that no Kestrel sandbox ever used (off-platform activity)
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

DEFAULT_N_S = 60
SENSITIVITY_NS = (5, 10, 15, 30, 45, 60, 90, 120, 300, 450, 600, 3600)


@dataclass
class Correlation:
    alerts: pd.DataFrame        # one row per alert with its status
    candidates: pd.DataFrame    # every (alert, flow) pair within N
    n_s: float

    def counts(self) -> pd.DataFrame:
        c = self.alerts.match_status.value_counts().reindex(["matched", "ambiguous", "unmatched"], fill_value=0)
        out = c.rename_axis("match_status").reset_index(name="alerts")
        out["share_pct"] = (100 * out.alerts / len(self.alerts)).round(2)
        return out


def key_pairs(alerts: pd.DataFrame, flows: pd.DataFrame) -> pd.DataFrame:
    """Every (alert, flow) pair that agrees on IPs and port, with delta_s = alert - flow."""
    a = alerts[["alert_id", "source_ip", "destination_host", "destination_port", "detected_at_utc"]].copy()
    f = flows[["flow_id", "session_id", "src_ip", "dst_ip", "dst_port", "flow_time_utc"]].copy()
    a["destination_port"] = a.destination_port.astype("int64")
    f["dst_port"] = f.dst_port.astype("int64")
    p = a.merge(f, left_on=["source_ip", "destination_host", "destination_port"],
                right_on=["src_ip", "dst_ip", "dst_port"])
    p["delta_s"] = (p.detected_at_utc - p.flow_time_utc).dt.total_seconds()
    return p[["alert_id", "flow_id", "session_id", "detected_at_utc", "flow_time_utc", "delta_s"]]


def _classify(alerts: pd.DataFrame, pairs: pd.DataFrame, n_s: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    cand = pairs[pairs.delta_s.abs() <= n_s]
    g = cand.groupby("alert_id")
    per = pd.DataFrame({"n_candidate_flows": g.size(), "n_candidate_sessions": g.session_id.nunique(),
                        "only_session": g.session_id.first(), "only_flow": g.flow_id.first(),
                        "only_delta_s": g.delta_s.first()})
    nearest = pairs.assign(abs_dt=pairs.delta_s.abs()).groupby("alert_id").abs_dt.min()

    out = alerts[["alert_id"]].join(per, on="alert_id")
    out["n_candidate_flows"] = out.n_candidate_flows.fillna(0).astype("int64")
    out["n_candidate_sessions"] = out.n_candidate_sessions.fillna(0).astype("int64")
    n = out.n_candidate_flows
    out["match_status"] = np.select([n == 1, n > 1], ["matched", "ambiguous"], "unmatched")
    out["flow_id"] = out.only_flow.where(n == 1)
    out["delta_s"] = out.only_delta_s.where(n == 1)
    out["session_id"] = out.only_session.where(out.n_candidate_sessions == 1)
    out["session_resolution"] = np.select([n == 1, out.n_candidate_sessions == 1], ["single_flow", "single_session"],
                                          "none")
    out["nearest_key_match_s"] = out.alert_id.map(nearest)        # diagnostics for the unmatched: NaN = no flow at all
    cols = ["alert_id", "match_status", "n_candidate_flows", "n_candidate_sessions", "flow_id", "session_id",
            "delta_s", "session_resolution", "nearest_key_match_s"]
    return out[cols].reset_index(drop=True), cand.reset_index(drop=True)


def correlate(alerts: pd.DataFrame, flows: pd.DataFrame, n_s: float = DEFAULT_N_S) -> Correlation:
    """Correlate accepted SIEM alerts with accepted flows within +-``n_s`` seconds."""
    per_alert, cand = _classify(alerts, key_pairs(alerts, flows), n_s)
    return Correlation(per_alert, cand, n_s)


def sensitivity(alerts: pd.DataFrame, flows: pd.DataFrame, ns=SENSITIVITY_NS) -> pd.DataFrame:
    """matched / ambiguous / unmatched / session-resolved counts for each candidate N."""
    pairs = key_pairs(alerts, flows)
    rows = []
    for n in ns:
        per, _ = _classify(alerts, pairs, n)
        c = per.match_status.value_counts()
        rows.append({"n_s": n, "matched": int(c.get("matched", 0)), "ambiguous": int(c.get("ambiguous", 0)),
                     "unmatched": int(c.get("unmatched", 0)), "alerts_with_session": int(per.session_id.notna().sum())})
    return pd.DataFrame(rows)
