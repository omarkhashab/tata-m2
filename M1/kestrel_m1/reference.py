"""Stage 2 reference data: clean, resolve and type the dimension sources.

Produces the dimensions every activity record is joined to:

* ``tenants``: one row per real tenant; aliases (tn_004 -> tn_003) in ``tenant_alias``
* ``agents``: Type-2 dimension, one row per (agent_id, release_version) with a surrogate
  ``agent_sk`` and a validity window [valid_from_utc, valid_to_utc)
* ``tools``: one row per (tool_id, tool_version); ``tool_alias`` maps tool names to ids
* ``pricing``: one row per (model_id, effective_from) with a derived ``effective_to_utc``
* ``policies``: one row per (sandbox_profile_id, policy_version) with an *effective*
  validity window (overlaps resolved: the newer version wins, as the sandbox itself did)

Nothing is dropped silently: every merged or removed source row is written to
``entity_resolution`` with its source file and line and the rule that was applied.
Each dimension keeps a pipe-separated ``quality_flags`` column for non-fatal issues.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import numpy as np
import pandas as pd

from . import readers
from .timeutil import UTC_DTYPE, parse_iso_utc

TENANT_REGIONS = {"eu-west-1", "us-east-1", "me-south-1"}
SEMVER_RX = r"\d+\.\d+\.\d+"


@dataclass
class Reference:
    tenants: pd.DataFrame
    tenant_alias: dict
    agents: pd.DataFrame
    tools: pd.DataFrame
    tool_alias: dict
    pricing: pd.DataFrame
    policies: pd.DataFrame
    entity_resolution: pd.DataFrame


# ------------------------------------------------------------------------ helpers

def _bool(s: pd.Series) -> pd.Series:
    """Pack booleans are lowercase ``true``/``false``; anything else becomes <NA>."""
    return s.map({"true": True, "false": False}).astype("boolean")


def add_flag(flags: pd.Series, mask, flag: str) -> pd.Series:
    mask = pd.Series(mask, index=flags.index).fillna(False).astype(bool)
    return flags.where(~mask, flags.where(flags == "", flags + "|") + flag)


def normalise_version(v: pd.Series) -> pd.Series:
    """``v2.3.1`` -> ``2.3.1`` (case-insensitive leading v), whitespace stripped."""
    return v.astype("string").str.strip().str.replace(r"^[vV]", "", regex=True)


def _semver_key(v: str) -> tuple:
    return tuple(int(p) for p in v.split(".")) if v and re.fullmatch(SEMVER_RX, v) else (-1,)


def _resolution_rows(entity: str, rows: pd.DataFrame, raw_col: str, resolved, rule: str, detail) -> list[dict]:
    resolved = resolved if isinstance(resolved, pd.Series) else pd.Series(resolved, index=rows.index)
    detail = detail if isinstance(detail, pd.Series) else pd.Series(detail, index=rows.index)
    return [{"entity": entity, "source_file": r.source_file, "source_line": int(r.source_line),
             "raw_key": str(getattr(r, raw_col)), "resolved_key": None if pd.isna(resolved[i]) else str(resolved[i]),
             "rule": rule, "detail": detail[i]}
            for i, r in zip(rows.index, rows.itertuples())]


# ------------------------------------------------------------------------ tenants

def clean_tenants(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict, list[dict]]:
    """Resolve duplicate tenants and validate enums.

    Two tenant ids are the same company when their normalised name matches (case,
    punctuation and legal suffixes ignored) or when security_contact *and*
    onboarded_at are identical. The canonical id is the lowest id of the group,
    which is also the one agents.csv references (tn_003, not tn_004).
    """
    df = raw.copy()
    df["onboarded_at_utc"] = parse_iso_utc(df.onboarded_at)
    df["is_active"] = _bool(df.is_active)
    flags = pd.Series("", index=df.index)

    bad_region = df.region.notna() & ~df.region.isin(TENANT_REGIONS)
    flags = add_flag(flags, bad_region, "INVALID_ENUM_REGION")
    df["region_raw"] = df.region
    df.loc[bad_region, "region"] = pd.NA
    flags = add_flag(flags, df.security_contact.isna(), "MISSING_SECURITY_CONTACT")
    df["quality_flags"] = flags

    name_norm = (df.tenant_name.str.lower()
                 .str.replace(r"\b(ltd|inc|llc|plc)\b\.?", "", regex=True)
                 .str.replace(r"[^a-z0-9]", "", regex=True))
    contact_key = df.security_contact.fillna("") + "|" + df.onboarded_at.fillna("")
    # union of the two matching rules: group ids that share either key, repeated until
    # stable so that a chain (A~B by contact, B~C by name) ends up in one group
    group = pd.Series(range(len(df)), index=df.index)
    keys = [k.fillna("__none__" + df.tenant_id) for k in (name_norm, contact_key.where(df.security_contact.notna()))]
    while True:
        before = group.copy()
        for key in keys:
            group = group.groupby(key).transform("min")
        if group.equals(before):
            break
    canonical = df.groupby(group).tenant_id.transform("min")
    alias = {t: c for t, c in zip(df.tenant_id, canonical) if t != c}

    merged = df[df.tenant_id != canonical]
    log = _resolution_rows("tenant", merged, "tenant_id", canonical[merged.index],
                           "same company: normalised name or identical security_contact+onboarded_at",
                           "merged into canonical tenant; its rows dropped from dim_tenant")
    cols = ["tenant_id", "tenant_name", "industry", "plan_tier", "region", "region_raw", "onboarded_at_utc",
            "is_active", "security_contact", "quality_flags", "source_file", "source_line"]
    out = df[df.tenant_id == canonical][cols].reset_index(drop=True)
    out["merged_tenant_ids"] = out.tenant_id.map(lambda t: "|".join(sorted(k for k, v in alias.items() if v == t)))
    return out, alias, log


# ------------------------------------------------------------------------- agents

def clean_agents(raw: pd.DataFrame, tenant_alias: dict, profile_ids: set, model_ids: set) -> tuple[pd.DataFrame, list[dict]]:
    """Normalise versions, drop duplicate versions, and build the Type-2 dimension.

    Duplicates on (agent_id, normalised version): the row whose version was already
    canonical (no ``v``) is kept, so its attributes win; the other row is logged with
    the attributes on which the two disagreed.

    Validity windows: a version is valid from its launch_date (00:00 UTC) until the
    next version of the same agent launches. Profiling 7.10 showed activity switches
    version exactly on those dates.
    """
    df = raw.copy()
    df["release_version_raw"] = df.release_version
    df["release_version"] = normalise_version(df.release_version)
    df["_prefixed"] = df.release_version != df.release_version_raw
    df = df.sort_values(["agent_id", "release_version", "_prefixed", "source_line"])

    dup = df.duplicated(["agent_id", "release_version"], keep="first")
    log = []
    attrs = ["agent_name", "agent_type", "model_id", "tenant_id", "sandbox_profile_id", "owner_team",
             "autonomy_level", "max_tool_calls_per_session", "launch_date", "is_deprecated"]
    for i in df.index[dup]:
        r = df.loc[i]
        kept = df[(df.agent_id == r.agent_id) & (df.release_version == r.release_version) & ~dup].iloc[0]
        diffs = {a: (kept[a], r[a]) for a in attrs if kept[a] != r[a]}
        log += _resolution_rows("agent_version", df.loc[[i]], "release_version_raw",
                                f"{r.agent_id}@{r.release_version}",
                                "duplicate (agent_id, version) after stripping 'v'; kept the canonical row",
                                "conflicting attributes kept/dropped: " + "; ".join(f"{k}={a!r}/{b!r}" for k, (a, b) in diffs.items()))
    df = df[~dup].copy()

    df["tenant_id_raw"] = df.tenant_id
    df["tenant_id"] = df.tenant_id.replace(tenant_alias)
    df["max_tool_calls_per_session"] = pd.to_numeric(df.max_tool_calls_per_session).astype("Int64")
    df["launch_date"] = pd.to_datetime(df.launch_date, format="%Y-%m-%d").dt.date
    df["is_deprecated"] = _bool(df.is_deprecated)

    flags = pd.Series("", index=df.index)
    flags = add_flag(flags, df._prefixed, "VERSION_PREFIX_STRIPPED")
    flags = add_flag(flags, ~df.sandbox_profile_id.isin(profile_ids), "NO_POLICY_PROFILE")
    flags = add_flag(flags, ~df.model_id.isin(model_ids), "UNKNOWN_MODEL")
    flags = add_flag(flags, ~df.release_version.str.fullmatch(SEMVER_RX).fillna(False), "NON_SEMVER_VERSION")
    df["quality_flags"] = flags

    # Type-2 windows, ordered by semantic version within each agent
    df["_semver"] = df.release_version.map(_semver_key)
    df = df.sort_values(["agent_id", "_semver"]).reset_index(drop=True)
    df["valid_from_utc"] = pd.to_datetime(df.launch_date.astype(str)).dt.tz_localize("UTC").astype(UTC_DTYPE)
    df["valid_to_utc"] = df.groupby("agent_id").valid_from_utc.shift(-1)
    df["is_current"] = df.valid_to_utc.isna()
    df.insert(0, "agent_sk", np.arange(1, len(df) + 1))

    cols = ["agent_sk", "agent_id", "release_version", "release_version_raw", "agent_name", "agent_type", "model_id",
            "tenant_id", "tenant_id_raw", "sandbox_profile_id", "owner_team", "autonomy_level",
            "max_tool_calls_per_session", "launch_date", "is_deprecated", "valid_from_utc", "valid_to_utc",
            "is_current", "quality_flags", "source_file", "source_line"]
    return df[cols], log


# -------------------------------------------------------------------------- tools

def clean_tools(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict, list[dict]]:
    """Deduplicate on (tool_id, tool_version) and build the tool-name alias map."""
    df = raw.copy()
    dup = df.duplicated(["tool_id", "tool_version"], keep="first")
    body_same = df.drop(columns=["source_file", "source_line"]).duplicated(keep="first")
    log = _resolution_rows("tool_version", df[dup], "tool_id",
                           (df.tool_id + "@" + df.tool_version)[dup],
                           "duplicate (tool_id, tool_version)",
                           np.where(body_same[dup], "identical row", "conflicting attributes; first row kept"))
    df = df[~dup].copy()
    df["requires_approval"] = _bool(df.requires_approval)

    cat_n = df.groupby("tool_id").tool_category.transform("nunique")
    df["quality_flags"] = add_flag(pd.Series("", index=df.index), cat_n > 1, "CATEGORY_DIFFERS_ACROSS_VERSIONS")

    # tool_name -> tool_id. Only names that identify exactly one tool are usable aliases.
    names = df.groupby("tool_name").tool_id.unique()
    alias = {n: ids[0] for n, ids in names.items() if len(ids) == 1}
    return df.reset_index(drop=True), alias, log


def resolve_tool_id(tool_id: pd.Series, alias: dict, known_ids: set) -> tuple[pd.Series, pd.Series]:
    """Map tool names that were written into ``tool_id`` back to the real id.

    Returns (resolved ids, mask of rows that were rewritten).
    """
    rewrite = tool_id.notna() & ~tool_id.isin(known_ids) & tool_id.isin(alias.keys())
    return tool_id.where(~rewrite, tool_id.map(alias)), rewrite


# ------------------------------------------------------------------------ pricing

def clean_pricing(raw: pd.DataFrame) -> pd.DataFrame:
    """Type prices, derive the missing effective_to from the next price point."""
    df = raw.copy()
    df["input_price_per_1k"] = pd.to_numeric(df.input_price_per_1k)
    df["output_price_per_1k"] = pd.to_numeric(df.output_price_per_1k)
    df["effective_from_utc"] = parse_iso_utc(df.effective_from)
    df["effective_to_declared_utc"] = parse_iso_utc(df.effective_to)
    df = df.sort_values(["model_id", "effective_from_utc"]).reset_index(drop=True)
    derived = df.groupby("model_id").effective_from_utc.shift(-1)
    df["effective_to_utc"] = df.effective_to_declared_utc.fillna(derived)
    flags = pd.Series("", index=df.index)
    flags = add_flag(flags, df.effective_to_declared_utc.isna() & derived.notna(), "EFFECTIVE_TO_DERIVED")
    flags = add_flag(flags, df.input_price_per_1k.isna() | df.output_price_per_1k.isna(), "PRICE_MISSING")
    df["quality_flags"] = flags
    return df[["model_id", "model_family", "input_price_per_1k", "output_price_per_1k", "currency",
               "effective_from_utc", "effective_to_utc", "quality_flags", "source_file", "source_line"]]


def attach_price(df: pd.DataFrame, model_col: str, time_col: str, pricing: pd.DataFrame) -> pd.DataFrame:
    """Add the price effective at each row's time (``effective_from <= t < effective_to``).

    Rows with no effective price (no model, before the first price point, or a price
    point with empty prices) get NaN prices; the caller flags them.
    """
    left = df[[model_col, time_col]].reset_index().rename(columns={"index": "_row"})
    left = left.dropna(subset=[model_col, time_col])
    left[time_col] = left[time_col].astype(UTC_DTYPE)          # merge_asof needs one resolution
    left[model_col] = left[model_col].astype(pricing.model_id.dtype)   # ... and one key dtype
    left = left.sort_values(time_col)
    right = pricing.sort_values("effective_from_utc")
    m = pd.merge_asof(left, right[["model_id", "effective_from_utc", "effective_to_utc",
                                   "input_price_per_1k", "output_price_per_1k"]],
                      left_on=time_col, right_on="effective_from_utc",
                      left_by=model_col, right_by="model_id", direction="backward")
    expired = m.effective_to_utc.notna() & (m[time_col] >= m.effective_to_utc)
    m.loc[expired, ["input_price_per_1k", "output_price_per_1k"]] = np.nan
    m = m.set_index("_row")[["input_price_per_1k", "output_price_per_1k", "effective_from_utc"]]
    m = m.rename(columns={"effective_from_utc": "price_effective_from_utc"})
    return df.join(m)


# ----------------------------------------------------------------------- policies

def clean_policies(raw: pd.DataFrame) -> pd.DataFrame:
    """Type the flattened policies and resolve overlapping validity windows.

    Overlap rule (finding PO1/PO1b): inside an overlap the newer policy_version
    applies, exactly as the sandbox did (its audit log reports v3 for every research
    decision inside the v2/v3 overlap). Implemented by cutting the older version's
    effective window at the newer version's valid_from. The declared window is kept.
    """
    df = raw.copy()
    df["policy_version"] = pd.to_numeric(df.policy_version).astype("Int64")
    df["valid_from_utc"] = parse_iso_utc(df.valid_from)
    df["valid_to_declared_utc"] = parse_iso_utc(df.valid_to)
    df = df.sort_values(["sandbox_profile_id", "policy_version"]).reset_index(drop=True)

    next_from = df.groupby("sandbox_profile_id").valid_from_utc.shift(-1)
    overlap = next_from.notna() & (df.valid_to_declared_utc.isna() | (df.valid_to_declared_utc > next_from))
    df["valid_to_utc"] = df.valid_to_declared_utc.where(~overlap, next_from)
    gap = next_from.notna() & df.valid_to_declared_utc.notna() & (df.valid_to_declared_utc < next_from)

    flags = pd.Series("", index=df.index)
    flags = add_flag(flags, overlap, "WINDOW_CUT_BY_NEWER_VERSION")
    flags = add_flag(flags, gap, "GAP_BEFORE_NEXT_VERSION")
    flags = add_flag(flags, ~df.has_network_block, "NO_NETWORK_BLOCK_NO_EGRESS")
    df["quality_flags"] = flags

    # without a network block nothing may leave the sandbox: empty allowlist, all ports denied
    for col in ["network_egress_allowlist", "network_denied_ports"]:
        df[col] = df[col].map(lambda v: v if isinstance(v, list) else [])
    for col in ["allowed_tools", "filesystem_read_scope", "filesystem_write_scope"]:
        df[col] = df[col].map(lambda v: v if isinstance(v, list) else [])
    return df[["sandbox_profile_id", "policy_version", "valid_from_utc", "valid_to_utc", "valid_to_declared_utc",
               "allowed_tools", "has_network_block", "network_egress_allowlist", "network_denied_ports",
               "network_max_egress_mb", "filesystem_read_scope", "filesystem_write_scope",
               "resources_max_runtime_s", "resources_max_memory_mb", "escalation_allowed",
               "quality_flags", "source_file", "source_line"]]


def attach_policy(df: pd.DataFrame, profile_col: str, time_col: str, policies: pd.DataFrame) -> pd.DataFrame:
    """Join each row to the policy version effective at its time (validity-window join).

    ``valid_from_utc <= t < valid_to_utc`` on the *effective* (overlap-resolved)
    window. Rows with no profile, an unknown profile, or a time outside every window
    get ``policy_version_applied`` = <NA>.
    """
    left = df[[profile_col, time_col]].reset_index().rename(columns={"index": "_row"})
    left = left.dropna(subset=[profile_col, time_col])
    left[time_col] = left[time_col].astype(UTC_DTYPE)          # merge_asof needs one resolution
    left[profile_col] = left[profile_col].astype(policies.sandbox_profile_id.dtype)   # ... and one key dtype
    left = left.sort_values(time_col)
    right = policies.sort_values("valid_from_utc")
    m = pd.merge_asof(left, right.drop(columns=["source_file", "source_line", "quality_flags"]),
                      left_on=time_col, right_on="valid_from_utc",
                      left_by=profile_col, right_by="sandbox_profile_id", direction="backward")
    outside = m.policy_version.isna() | (m.valid_to_utc.notna() & (m[time_col] >= m.valid_to_utc))
    m = m[~outside].set_index("_row")
    keep = [c for c in m.columns if c not in (profile_col, time_col, "sandbox_profile_id")]
    m = m[keep].rename(columns={"policy_version": "policy_version_applied"})
    out = df.join(m)
    out["policy_version_applied"] = out["policy_version_applied"].astype("Int64")
    return out


# --------------------------------------------------------------------- agent keys

def attach_agent_sk(df: pd.DataFrame, agents: pd.DataFrame, time_col: str,
                    agent_col: str = "agent_id", version_col: str = "release_version") -> pd.DataFrame:
    """Resolve ``agent_sk`` from (agent_id, normalised release_version).

    The record names the version that ran, so that is the primary key; the Type-2
    window is a cross-check: ``agent_version_outside_window`` is True when the
    record's time is outside that version's [valid_from, valid_to).
    """
    v = normalise_version(df[version_col])
    key = df[[agent_col]].assign(_v=v)
    dim = agents[["agent_sk", "agent_id", "release_version", "valid_from_utc", "valid_to_utc",
                  "sandbox_profile_id", "tenant_id"]].rename(
        columns={"agent_id": agent_col, "release_version": "_v", "tenant_id": "agent_tenant_id"})
    m = key.reset_index().merge(dim, on=[agent_col, "_v"], how="left").set_index("index")
    t = df[time_col]
    outside = m.agent_sk.notna() & t.notna() & ((t < m.valid_from_utc) | (m.valid_to_utc.notna() & (t >= m.valid_to_utc)))
    out = df.copy()
    out["release_version_norm"] = v
    out["agent_sk"] = m.agent_sk.astype("Int64")
    out["sandbox_profile_id"] = m.sandbox_profile_id
    out["agent_tenant_id"] = m.agent_tenant_id
    out["agent_version_outside_window"] = outside.fillna(False)
    return out


# -------------------------------------------------------------------------- build

def load_reference() -> Reference:
    """Read and clean all reference sources in dependency order."""
    raw_policies = readers.read_sandbox_policies()
    raw_pricing = readers.read_model_pricing()
    tenants, tenant_alias, log_t = clean_tenants(readers.read_tenants())
    policies = clean_policies(raw_policies)
    pricing = clean_pricing(raw_pricing)
    agents, log_a = clean_agents(readers.read_agents(), tenant_alias,
                                 set(policies.sandbox_profile_id), set(pricing.model_id))
    tools, tool_alias, log_tl = clean_tools(readers.read_tools())
    er = pd.DataFrame(log_t + log_a + log_tl,
                      columns=["entity", "source_file", "source_line", "raw_key", "resolved_key", "rule", "detail"])
    # tools known under several names are entity resolution too (aliases applied to activity later)
    multi = tools.groupby("tool_id").tool_name.nunique()
    alias_rows = pd.DataFrame([{"entity": "tool_name", "source_file": r.source_file, "source_line": int(r.source_line),
                                "raw_key": r.tool_name, "resolved_key": r.tool_id,
                                "rule": "one tool under several names: name -> tool_id",
                                "detail": f"version {r.tool_version}"}
                               for r in tools[tools.tool_id.isin(multi[multi > 1].index)].itertuples()])
    er = pd.concat([er, alias_rows], ignore_index=True)
    return Reference(tenants, tenant_alias, agents, tools, tool_alias, pricing, policies, er)
