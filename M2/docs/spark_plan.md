# Spark physical plan: analysis a01, and what the skew response changed

Analysis **a01** gives daily session volume and policy-violation rate by agent version (DataFrame API,
`kestrel_m2/spark/analytics.py`). It unions three facts (`fact_tool_call`, `fact_network_flow`, `fact_session`),
joins them to `dim_agent` and `dim_date`, and aggregates per day and agent version. It is the analysis that meets the
documented skew head-on, because its join key `(agent_id, release_version)` is the skewed one.

The plans are printed with `df.explain(mode="formatted")` and saved verbatim:

| file | plan |
|---|---|
| [`output/spark/explain_a01_baseline.txt`](../output/spark/explain_a01_baseline.txt) | no skew response: auto-broadcast and AQE off, so the facts are shuffled on the agent key |
| [`output/spark/explain_a01_skew_response.txt`](../output/spark/explain_a01_skew_response.txt) | **the plan we run**: the dimensions are broadcast |
| [`output/spark/explain_a01_salted_join.txt`](../output/spark/explain_a01_salted_join.txt) | the fallback response when a dimension is too big to broadcast: a salted shuffle join |

## The plan we run

```
* Sort (32)
+- Exchange (31)                                   rangepartitioning(day, agent_id, release_version, 16)
   +- * Project (30)
      +- * HashAggregate (29)                      final sums
         +- Exchange (28)                          hashpartitioning(day, agent_id, release_version, agent_name, agent_type, 16)
            +- * HashAggregate (27)                partial_sum(...)  (map-side)
               +- * Project (26)
                  +- * BroadcastHashJoin Inner BuildRight (25)      date_key = date_key
                     :- * Project (19)
                     :  +- * BroadcastHashJoin Inner BuildRight (18) (agent_id, release_version)
                     :     :- Union (13)
                     :     :  :- Scan parquet fact_tool_call (1)
                     :     :  :- Scan parquet fact_network_flow (5)
                     :     :  +- Scan parquet fact_session (9)
                     :     +- BroadcastExchange (17)  HashedRelationBroadcastMode(agent_id, release_version)
                     :        +- Scan parquet dim_agent (14)
                     +- BroadcastExchange (24)        HashedRelationBroadcastMode(date_key)
                        +- Scan parquet dim_date (20)
```

**What is scanned.** Five Parquet files from the warehouse mirror. The three facts are read with column pruning
(`ReadSchema: struct<agent_id, release_version, date_key, is_policy_violation>`: 4 of the 32 columns of
`fact_tool_call`). `event_id`, selected in the code, is pruned because nothing downstream uses it. The scans carry
`PushedFilters: [IsNotNull(agent_id), IsNotNull(release_version), IsNotNull(date_key)]`, which Spark inferred from
the inner joins and pushed into the Parquet reader. The union feeds 461,669 rows (319,361 + 42,308 + 100,000) into the
joins.

**How the join is executed.** Both joins are `BroadcastHashJoin Inner BuildRight`. `dim_agent` (21 rows) and
`dim_date` (61 rows) are collected once, built into hash relations (`BroadcastExchange`,
`HashedRelationBroadcastMode`) and shipped to every task. Each task then joins its own split of the fact union
locally, in the same whole-stage-codegen stage (`codegen id : 6`) as the scan, so the fact rows are never moved to
join them.

**Where the shuffles are.** There are two exchanges and both are small. (28) `hashpartitioning(day, agent_id,
release_version, agent_name, agent_type, 16)` runs after the partial `HashAggregate (27)`, so each task sends at most
one row per (day, agent version) instead of its raw rows: 805 groups in total. (31) is the
`rangepartitioning` for the final `orderBy`, over those 805 rows.

## What the skew response changed

The pack is skewed on purpose: `ag_004@3.0.2` owns 34.3% of the tool calls, 32.6% of the prompt turns and 26.8% of
the sessions, and `tn_001` about 62% of everything ([`output/spark/skew_report.csv`](../output/spark/skew_report.csv)).

* **Baseline** (`explain_a01_baseline.txt`). Each join is a `SortMergeJoin` with
  `Exchange hashpartitioning(agent_id, release_version, 16)` (14) on all 461,669 fact rows, followed by a second full
  shuffle on `date_key` (23, 29) for the date join: four exchanges of the whole fact union before aggregating. Hashing
  on the agent key sends every `ag_004@3.0.2` row to the same reducer. For `fact_tool_call` we measured
  **109,559 of 319,361 rows (34.3%) in one of 16 reducers**, a median of 14,059, and 3 reducers empty. The straggler
  sets the stage time.
* **Skew response** (the plan above). Broadcasting the dimensions removes both full shuffles, so the skewed key never
  decides where a fact row goes. The hot key now costs one partial-aggregate row per input task, and nothing more.
  The same query ran in **0.98 s instead of 6.81 s** (local[*], AQE off for a like-for-like comparison).
* **When a dimension cannot be broadcast** (`explain_a01_salted_join.txt`). Each fact row gets
  `salt = pmod(xxhash64(event_id), 8)`, and the dimension is replicated once per salt (a cross join with `range(8)`). The
  join shuffle becomes `hashpartitioning(agent_id, release_version, salt, 16)`, and the largest reducer drops to
  **38,502 rows (12.1%)**, with no empty reducer.
* All three plans give **identical results** (805 rows, asserted in the run). AQE's skew-join splitting
  (`spark.sql.adaptive.skewJoin.enabled`) stays on in every other analysis as a safety net.
