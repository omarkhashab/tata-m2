"""Lab 6: flatten ``sandbox_policies.json`` in DuckDB (not by hand) and load it as ``dim_policy``.

DuckDB reads the nested array with ``read_json`` and infers ``network``, ``filesystem``
and ``resources`` as STRUCTs and the allowlists as LISTs, so flattening is plain SQL:
struct fields are selected with dot notation, lists are kept as arrays for
``dim_policy`` and ``UNNEST``-ed into two bridge tables:

* ``policy_allowed_tool``   one row per (profile, version, tool_id)
* ``policy_egress_rule``    one row per (profile, version, allowlist entry), with
  ``is_wildcard`` and ``match_regex``, the anchored regex of M1's rule (a ``*`` matches
  exactly one DNS label, case-insensitive), usable as ``regexp_matches(lower(host), match_regex)``

The overlap rule of M1 (finding PO1b: inside an overlap the newer version wins) is a
``LEAD`` window function here. The result is asserted equal to M1's pandas flatten
(``kestrel_m1.reference.clean_policies``), so both implementations agree.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from . import config

FLATTEN_SQL = """
WITH raw AS (
    SELECT * FROM read_json(?, format = 'array', maximum_object_size = 16777216)
), flat AS (
    SELECT
        sandbox_profile_id,
        CAST(policy_version AS INTEGER)                              AS policy_version,
        CAST(valid_from AS TIMESTAMPTZ)                              AS valid_from_utc,
        CAST(valid_to AS TIMESTAMPTZ)                                AS valid_to_declared_utc,
        allowed_tools,
        network IS NOT NULL                                          AS has_network_block,
        COALESCE(network.egress_allowlist, CAST([] AS VARCHAR[]))    AS egress_allowlist,
        COALESCE(network.denied_ports, CAST([] AS BIGINT[]))         AS denied_ports,
        CAST(network.max_egress_mb AS DOUBLE)                        AS max_egress_mb,
        filesystem.read_scope                                        AS fs_read_scope,
        filesystem.write_scope                                       AS fs_write_scope,
        CAST(resources.max_runtime_s AS INTEGER)                     AS max_runtime_s,
        CAST(resources.max_memory_mb AS INTEGER)                     AS max_memory_mb,
        escalation_allowed,
        LEAD(CAST(valid_from AS TIMESTAMPTZ)) OVER (
            PARTITION BY sandbox_profile_id ORDER BY policy_version) AS next_valid_from
    FROM raw
)
SELECT
    * EXCLUDE (next_valid_from),
    CASE WHEN next_valid_from IS NOT NULL
              AND (valid_to_declared_utc IS NULL OR valid_to_declared_utc > next_valid_from)
         THEN next_valid_from ELSE valid_to_declared_utc END          AS valid_to_utc,
    concat_ws('|',
        CASE WHEN next_valid_from IS NOT NULL
                  AND (valid_to_declared_utc IS NULL OR valid_to_declared_utc > next_valid_from)
             THEN 'WINDOW_CUT_BY_NEWER_VERSION' END,
        CASE WHEN next_valid_from IS NOT NULL AND valid_to_declared_utc < next_valid_from
             THEN 'GAP_BEFORE_NEXT_VERSION' END,
        CASE WHEN NOT has_network_block THEN 'NO_NETWORK_BLOCK_NO_EGRESS' END) AS quality_flags
FROM flat
ORDER BY sandbox_profile_id, policy_version
"""

ALLOWED_TOOL_SQL = """
SELECT sandbox_profile_id, policy_version, UNNEST(allowed_tools) AS tool_id
FROM policy ORDER BY 1, 2, 3
"""

EGRESS_RULE_SQL = r"""
SELECT sandbox_profile_id, policy_version, entry AS allowlist_entry,
       starts_with(entry, '*.')                     AS is_wildcard,
       '^' || replace(replace(lower(entry), '.', '\.'), '*', '[^.]+') || '$' AS match_regex
FROM (SELECT sandbox_profile_id, policy_version, UNNEST(egress_allowlist) AS entry FROM policy)
ORDER BY 1, 2, 3
"""


def flatten(path: Path | None = None) -> dict[str, pd.DataFrame]:
    """dim_policy + the two bridge tables, all computed inside DuckDB."""
    path = Path(path or config.RAW_DIR / "sandbox_policies.json")
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")
        policy = con.execute(FLATTEN_SQL, [str(path)]).df()
        con.register("policy", policy)
        allowed = con.execute(ALLOWED_TOOL_SQL).df()
        egress = con.execute(EGRESS_RULE_SQL).df()
    finally:
        con.close()
    for c in ("valid_from_utc", "valid_to_declared_utc", "valid_to_utc"):
        policy[c] = pd.to_datetime(policy[c], utc=True).astype("datetime64[ms, UTC]")
    policy["policy_version"] = policy.policy_version.astype("int64")
    policy["denied_ports"] = policy.denied_ports.map(lambda v: [int(x) for x in v])
    for c in ("allowed_tools", "egress_allowlist", "fs_read_scope", "fs_write_scope"):
        policy[c] = policy[c].map(list)
    allowed["policy_version"] = allowed.policy_version.astype("int64")
    egress["policy_version"] = egress.policy_version.astype("int64")
    return {"dim_policy": policy, "policy_allowed_tool": allowed, "policy_egress_rule": egress}


def check_against_m1(duck: pd.DataFrame, m1: pd.DataFrame) -> pd.DataFrame:
    """Field-by-field comparison with M1's pandas flatten; returns the mismatches (empty = equal)."""
    pairs = {"valid_from_utc": "valid_from_utc", "valid_to_utc": "valid_to_utc",
             "valid_to_declared_utc": "valid_to_declared_utc", "allowed_tools": "allowed_tools",
             "has_network_block": "has_network_block", "egress_allowlist": "network_egress_allowlist",
             "denied_ports": "network_denied_ports", "fs_read_scope": "filesystem_read_scope",
             "fs_write_scope": "filesystem_write_scope", "max_runtime_s": "resources_max_runtime_s",
             "max_memory_mb": "resources_max_memory_mb", "escalation_allowed": "escalation_allowed",
             "quality_flags": "quality_flags"}
    m = duck.merge(m1.assign(policy_version=m1.policy_version.astype("int64")),
                   on=["sandbox_profile_id", "policy_version"], how="outer", suffixes=("_duck", "_m1"),
                   indicator=True)
    rows = []
    for d in m.to_dict("records"):
        if d["_merge"] != "both":
            rows.append({"sandbox_profile_id": d["sandbox_profile_id"], "policy_version": d["policy_version"],
                         "field": "<row>", "duckdb": d["_merge"], "m1": d["_merge"]})
            continue
        for a, b in pairs.items():
            x = d.get(a + "_duck", d.get(a))
            y = d.get(b + "_m1", d.get(b))
            same = (pd.isna(x) and pd.isna(y)) if not isinstance(x, (list, tuple)) and not isinstance(y, (list, tuple)) \
                else list(x) == list(y)
            if not same and not (not isinstance(x, (list, tuple)) and x == y):
                rows.append({"sandbox_profile_id": d["sandbox_profile_id"], "policy_version": d["policy_version"],
                             "field": a, "duckdb": x, "m1": y})
    return pd.DataFrame(rows, columns=["sandbox_profile_id", "policy_version", "field", "duckdb", "m1"])
