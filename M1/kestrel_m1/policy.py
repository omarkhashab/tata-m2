"""Stage 2 derivations: policy in effect, violations, risk indicators, cost.

Every record is judged against the policy version whose effective window
``[valid_from, valid_to)`` contains its own UTC time (``reference.attach_policy``),
never against the current version. Inside the sp_research_std v2/v3 overlap the
newer version applies (finding PO1b).

``is_policy_violation`` per activity type:

* tool call: ``tool_id`` not in the policy's ``allowed_tools``
* network flow: no network block at all (no egress allowed), or a denied port, or a
  destination host that matches no allowlist entry. Wildcards are matched as patterns:
  ``*.public-data.example`` matches exactly one extra DNS label (``census.public-data.example``),
  not the apex and not deeper names, case-insensitively. A flow whose host is unknown
  (IP only) cannot be shown to be allowlisted and counts as a violation (deny by default).
* sandbox decision: the sandbox said DENY, or it is a socket_connect that breaks the
  egress rule above although the sandbox allowed it (the sandbox fails open on
  NO_NETWORK_POLICY and WILDCARD_MATCH_UNVERIFIED; we judge by the policy)
* lifecycle / prompt turn: <NA>, no policy rule applies to them
* any record with no policy in effect (agent without a known sandbox profile): <NA>,
  flagged NO_POLICY_IN_EFFECT. A violation is not invented for a missing policy.

``risk_indicators`` is the pipe-separated union of injection indicators (prompt turns),
denials and violations, using the token vocabulary in ``RISK_TOKENS``.

Cost (prompt turns only): ``tokens_in/1000 * input_price + tokens_out/1000 * output_price``
at the price effective at the turn's time, rounded to 6 decimals (USD). Missing-token
policy = null-excluded: a turn with no output_tokens, or no effective price, has
cost_usd <NA> and is left out of cost sums. It is never imputed as 0.
"""

from __future__ import annotations

import re
from functools import lru_cache

import numpy as np
import pandas as pd

from .reference import Reference, add_flag, attach_policy, attach_price

RISK_TOKENS = {
    "tool_not_allowed": "tool call outside the policy's allowed_tools",
    "tool_denied": "tool call status = denied",
    "egress_not_allowlisted": "flow destination matches no allowlist entry",
    "egress_host_unknown": "flow destination host unknown (IP only), cannot be allowlisted",
    "port_denied": "flow to a port in the policy's denied_ports",
    "no_egress_allowed": "flow under a policy version with no network block",
    "egress_blocked": "flow verdict = blocked",
    "sandbox_denied": "sandbox decision = DENY (plus the lower-cased sandbox reason)",
    "allowed_despite_policy": "sandbox ALLOWed a connection the effective policy forbids (fail-open)",
    "escalation_attempt": "setuid / ptrace / mount syscall attempted",
}
INJECTION_INDICATORS = {"instruction_override", "role_confusion", "exfil_request", "encoded_payload",
                        "tool_coercion", "policy_probe"}
ESCALATION_ACTIONS = {"setuid", "ptrace", "mount"}


# --------------------------------------------------------------------- helpers

@lru_cache(maxsize=None)
def _pattern(entry: str) -> re.Pattern:
    return re.compile(re.escape(entry.lower()).replace(r"\*", r"[^.]+"))


def host_allowed(host, allowlist) -> bool:
    """True when ``host`` matches an allowlist entry (exact name or one-label wildcard)."""
    if host is None or (isinstance(host, float) and np.isnan(host)) or host is pd.NA:
        return False
    h = str(host).lower()
    return any(_pattern(e).fullmatch(h) for e in allowlist)


def _join_tokens(parts: list[pd.Series], index) -> pd.Series:
    """Union of pipe-separated token columns, de-duplicated, order of first appearance."""
    out = pd.Series("", index=index, dtype="string")
    for p in parts:
        p = p.fillna("").astype("string")
        out = out.where(p == "", out.where(out == "", out + "|") + p)
    return out.map(lambda s: "|".join(dict.fromkeys(t for t in s.split("|") if t)))


def _token(mask, token: str, index) -> pd.Series:
    mask = pd.Series(mask, index=index).fillna(False).astype(bool)
    return pd.Series(np.where(mask, token, ""), index=index, dtype="string")


def _with_policy(df: pd.DataFrame, time_col: str, ref: Reference) -> pd.DataFrame:
    """Window-join the policy; ``df`` must carry ``sandbox_profile_id``."""
    return attach_policy(df, "sandbox_profile_id", time_col, ref.policies)


# -------------------------------------------------------------------- activity

def enrich_activity(acc: pd.DataFrame, ref: Reference) -> pd.DataFrame:
    """Policy, violation, risk indicators, tokens and cost for accepted activity rows."""
    df = _with_policy(acc, "event_time_utc", ref)
    idx = df.index
    has_policy = df.policy_version_applied.notna()
    is_tool = df.event_type == "tool.call"
    is_turn = df.event_type == "llm.turn"

    allowed = pd.Series([t in a if isinstance(a, list) else False for t, a in zip(df.tool_id, df.allowed_tools)],
                        index=idx)
    tool_violation = is_tool & has_policy & ~allowed
    df["is_policy_violation"] = pd.Series(pd.NA, index=idx, dtype="boolean")
    df.loc[is_tool & has_policy, "is_policy_violation"] = tool_violation[is_tool & has_policy]

    indicators = df.injection_indicators.where(is_turn, "")
    df["risk_indicators"] = _join_tokens([indicators, _token(tool_violation, "tool_not_allowed", idx),
                                          _token(is_tool & (df.status == "denied"), "tool_denied", idx)],
                                         idx)

    # tokens and cost: prompt turns only
    df["tokens_in"] = df.input_tokens.where(is_turn)
    df["tokens_out"] = df.output_tokens.where(is_turn)
    priced = attach_price(df[is_turn][["model_id", "event_time_utc"]], "model_id", "event_time_utc", ref.pricing)
    df["input_price_per_1k"] = priced.input_price_per_1k.reindex(idx)
    df["output_price_per_1k"] = priced.output_price_per_1k.reindex(idx)
    cost = (df.tokens_in.astype("Float64") / 1000 * df.input_price_per_1k
            + df.tokens_out.astype("Float64") / 1000 * df.output_price_per_1k)
    df["cost_usd"] = cost.round(6).where(is_turn)

    f = df.quality_flags
    f = add_flag(f, ~has_policy, "NO_POLICY_IN_EFFECT")
    f = add_flag(f, is_turn & df.input_price_per_1k.isna(), "PRICE_MISSING")
    df["quality_flags"] = f
    return df.drop(columns=_policy_detail_cols(df))


# ---------------------------------------------------------------------- egress

def egress_rules(df: pd.DataFrame, host: pd.Series, port: pd.Series, applies: pd.Series) -> tuple[pd.Series, pd.Series]:
    """The egress rule for rows with a window-joined policy, shared by flows and audit.

    ``host`` is the destination name (<NA> when only an IP is known), ``port`` an int.
    Returns (violation, violation_rule); both are <NA> / "" where ``applies`` is False.
    """
    idx = df.index
    has_policy = applies & df.policy_version_applied.notna()
    no_egress = has_policy & ~df.has_network_block.fillna(True).astype(bool)
    port_denied = has_policy & pd.Series(
        [pd.notna(p) and int(p) in d if isinstance(d, list) else False
         for p, d in zip(port, df.network_denied_ports)], index=idx)
    host_ok = pd.Series([host_allowed(h, a) if isinstance(a, list) else False
                         for h, a in zip(host, df.network_egress_allowlist)], index=idx)
    unknown_host = host.isna()
    not_listed = has_policy & ~no_egress & ~host_ok & ~unknown_host
    host_unknown = has_policy & ~no_egress & unknown_host
    violation = (no_egress | port_denied | not_listed | host_unknown).astype("boolean").where(has_policy)
    rule = _join_tokens([_token(no_egress, "no_egress_allowed", idx), _token(port_denied, "port_denied", idx),
                         _token(not_listed, "egress_not_allowlisted", idx),
                         _token(host_unknown, "egress_host_unknown", idx)], idx)
    return violation, rule


# ----------------------------------------------------------------------- flows

def enrich_flows(fl: pd.DataFrame, ref: Reference) -> pd.DataFrame:
    df = _with_policy(fl, "flow_time_utc", ref)
    idx = df.index
    df["is_policy_violation"], df["violation_rule"] = egress_rules(df, df.dst_host, df.dst_port,
                                                                   pd.Series(True, index=idx))
    df["risk_indicators"] = _join_tokens([df.violation_rule, _token(df.verdict == "blocked", "egress_blocked", idx)], idx)
    df["quality_flags"] = add_flag(df.quality_flags, df.policy_version_applied.isna(), "NO_POLICY_IN_EFFECT")
    return df.drop(columns=_policy_detail_cols(df))


# ----------------------------------------------------------------------- audit

IPV4_FULL_RX = r"(?:\d{1,3}\.){3}\d{1,3}"


def enrich_audit(au: pd.DataFrame, ref: Reference) -> pd.DataFrame:
    """A decision is a violation when the sandbox denied it, or when it is a
    socket_connect the policy forbids even though the sandbox allowed it (fail-open:
    NO_NETWORK_POLICY and WILDCARD_MATCH_UNVERIFIED lines). We judge against the policy,
    not against the enforcement point that may itself be wrong.
    """
    df = _with_policy(au, "event_time_utc", ref)
    idx = df.index
    has_policy = df.policy_version_applied.notna()
    deny = df.decision == "DENY"
    sock = df.action == "socket_connect"
    parts = df.target.where(sock).str.rsplit(":", n=1, expand=True)
    host = parts[0].where(~parts[0].str.fullmatch(IPV4_FULL_RX).fillna(False))
    port = pd.to_numeric(parts[1], errors="coerce").astype("Int64")
    egress_violation, rule = egress_rules(df, host, port, sock)
    fail_open = ~deny & egress_violation.fillna(False).astype(bool)

    violation = (deny | fail_open).astype("boolean")
    df["is_policy_violation"] = violation.where(has_policy | deny)
    df["violation_rule"] = _join_tokens([df.reason.str.lower().where(deny, ""), rule], idx)
    df["risk_indicators"] = _join_tokens([_token(deny, "sandbox_denied", idx), df.violation_rule,
                                          _token(fail_open, "allowed_despite_policy", idx),
                                          _token(df.action.isin(ESCALATION_ACTIONS), "escalation_attempt", idx)], idx)

    reported = df.policy_version_reported
    f = df.quality_flags
    f = add_flag(f, reported.isna() & has_policy, "POLICY_VERSION_DERIVED")
    f = add_flag(f, reported.notna() & has_policy & (reported != df.policy_version_applied), "POLICY_VERSION_MISMATCH")
    f = add_flag(f, ~has_policy, "NO_POLICY_IN_EFFECT")
    df["quality_flags"] = f
    return df.drop(columns=_policy_detail_cols(df))


def _policy_detail_cols(df: pd.DataFrame) -> list[str]:
    """Policy attributes used for the decision; the canonical table keeps only the version."""
    keep = {"policy_version_applied"}
    cols = ["valid_from_utc", "valid_to_utc", "valid_to_declared_utc", "allowed_tools", "has_network_block",
            "network_egress_allowlist", "network_denied_ports", "network_max_egress_mb", "filesystem_read_scope",
            "filesystem_write_scope", "resources_max_runtime_s", "resources_max_memory_mb", "escalation_allowed"]
    return [c for c in cols if c in df.columns and c not in keep]


# ---------------------------------------------------------------- cross-checks

def cross_check(activity: pd.DataFrame, flows: pd.DataFrame, audit: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Compare our derived verdicts with what the sandbox itself logged.

    * tool calls: our tool_not_allowed vs. an audit TOOL_NOT_ALLOWED line for the same
      session and target
    * flows: our egress verdict vs. the audit socket_connect decision for the same
      session and target (host:port or ip:port)
    * audit: the policy version the sandbox reported vs. our window-join version
    """
    out = {}
    tc = activity[activity.event_type == "tool.call"]
    tna = audit[audit.reason == "TOOL_NOT_ALLOWED"][["session_id", "target"]].drop_duplicates().assign(sandbox_tool_denied=True)
    m = tc.merge(tna, left_on=["session_id", "target_resource"], right_on=["session_id", "target"], how="left")
    out["tool_calls"] = pd.crosstab(m.is_policy_violation.astype("string").fillna("<NA>"),
                                    m.sandbox_tool_denied.fillna(False).astype(bool), margins=True)

    sc = audit[audit.action == "socket_connect"][["session_id", "target", "decision", "reason"]]
    f = flows.assign(t_host=flows.dst_host.fillna("") + ":" + flows.dst_port.astype("string"),
                     t_ip=flows.dst_ip + ":" + flows.dst_port.astype("string"))
    m = pd.concat([f.merge(sc, left_on=["session_id", k], right_on=["session_id", "target"]) for k in ("t_host", "t_ip")], ignore_index=True)
    m = m.drop_duplicates("flow_id")
    out["flows"] = pd.crosstab([m.decision, m.reason], m.is_policy_violation.astype("string").fillna("<NA>"), margins=True)
    out["flows_verdict"] = pd.crosstab(flows.verdict, flows.is_policy_violation.astype("string").fillna("<NA>"), margins=True)

    rep = audit.policy_version_reported.astype("string").fillna("-")
    out["audit_versions"] = pd.crosstab([audit.sandbox_profile_id.fillna("<none>"), rep],
                                        audit.policy_version_applied.astype("string").fillna("<NA>"))
    return out
