<div align="center">

# Pipelines

**Repairing a nightly feature pipeline that works and is wrong anyway**

A food delivery company builds its restaurant feature table every night, and both the real-time ETA service and the weekly retrain read it.<br>
The job ran, its happy-path test passed, and it still failed in four ways nobody could see by reading it.<br>
I diagnosed each one, made it survivable, and traced which data and which models the damage reached.

![Python](https://img.shields.io/badge/Python_3.13-3776AB?style=flat-square&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB_1.5-FFF000?style=flat-square&logo=duckdb&logoColor=black)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat-square)
![Parquet](https://img.shields.io/badge/Apache_Parquet-50ABF1?style=flat-square&logo=apacheparquet&logoColor=white)
![uv](https://img.shields.io/badge/uv-DE5FE9?style=flat-square&logo=uv&logoColor=white)
![Docker](https://img.shields.io/badge/Docker_Compose-2496ED?style=flat-square&logo=docker&logoColor=white)

<img src="https://github.com/user-attachments/assets/cd8e73ec-e57b-4c52-8218-1028026205d9" width="90%" alt="Input events on March 9 and March 10">

<sub>Input events on a normal day (top: March 9) and the night of the upstream change (bottom: March 10). <code>prep_time_minutes</code> jumps about 60x; everything else stays put.</sub>

</div>

<br>

## Overview

- **Period** : 2026.09 – 2026.10 · Boise State University, CS 459 Machine Learning in Production
- **Data** : synthetic food delivery events, 946,698 orders and 918,220 deliveries over 80 days (2026-01-01 – 2026-03-21), about 400 restaurants
- **Pipeline** : a nightly batch job in DuckDB that writes one Hive-style Parquet partition per date

The original assignment text is in [`README/SPEC.md`](README/SPEC.md).

<br>

## The Situation

I had just joined the ML platform team at a food delivery company, and the nightly job that builds the restaurant feature table (`aggs`) was now mine. It feeds two things: the **50 ms request path** that shows customers an ETA, and the **training pipeline** that retrains the ETA model every week.

The job works, and its happy-path test passes. It also failed four different ways in my first week, and each failure has a test in the repo. I was not building a pipeline from scratch; I was **diagnosing and repairing** one that someone else wrote. Most of the fixes are small. Finding them, and understanding why they matter, was the work.

<br>

## What Had to Be Decided

1. Why does a retry double the data, and how do I make a re-run safe?
2. Which information leaks into a feature that is supposed to use only the past?
3. An upstream team changed its data without telling anyone. How should the pipeline catch it, and what should happen when it does?
4. A bad commit was live for a week. Which partitions are wrong, which do I repair, and which models learned from them?
5. What is the pipeline still vulnerable to after all this?

<br>

## The Pipeline

```
read_events ──> join_deliveries ──> [ gates ] ──> rolling_aggs ──> write_aggs ──> aggs/dt=D
                                        │
                                        └── fail: write nothing · warn: write + record · quarantine: write to quarantine/
```

| Stage | What it does |
|---|---|
| 1. `read_events` | Reads the order and delivery events for the date |
| 2. `join_deliveries` | Joins each delivery to its order |
| gates | Check that day's joined deliveries before anything is aggregated or written |
| 3. `rolling_aggs` | Computes each restaurant's trailing 30-day features (delivery count, average prep time, average delivery time, late rate) |
| 4. `write_aggs` | Writes the date's partition to `aggs/dt=D` |

The runner retries a failed stage up to three times and writes a **run manifest** for every run: run ID, git SHA, timestamps, which partitions it read and which it wrote.

<br>

## How I Worked Through It

### 0. Predict before reading the code

Before opening any code, I wrote one prediction per failing test, and kept them after I learned the answers.

| Part | What the test said | My prediction | Turned out |
|---|---|---|---|
| 1 | Running the same date twice doubled the rows | The partition accumulates on every run | Right |
| 2 | R017's 30-day prep average was 33.715, but should have been 33.0876 | Data from other dates is mixed in | Close: it was the **same** day, not other days |
| 3 | From 3/10, `prep_time_minutes` was about 60x larger; the row-count gate passed | The upstream unit changed from minutes to seconds | Right |
| 4 | `partition_status.sql` and `contaminated_models.sql` were missing | These are the queries I have to write to find the damage | Right |

### 1. It ran twice: making a re-run safe

> The 2 a.m. job failed partway through, the automatic retry succeeded at 2:45, and the next morning March 3 had two copies of every row. Nothing errored.

**Cause.** Stage 4 wrote with DuckDB's `APPEND` option, which adds a new file to `aggs/dt=D` and leaves the old ones in place. Every run of the same date added another full copy.

**Where the assumption lives.** `run_stage()` in `src/runner.py` simply calls the stage again on failure. That only works if every stage is **idempotent**: running it twice must leave the same result as running it once.

| Write option | Same date twice | Backfill over 14 dates | |
|---|---|---|---|
| `APPEND` (original) | Rows double | Every date doubles on the second pass | X |
| `OVERWRITE` (my first try) | No duplicates | Wipes the **whole** `aggs/` folder on every write, so only the last date survives | X |
| `OVERWRITE_OR_IGNORE` | No duplicates | Replaces only the partition being written; other dates are left alone | O |

```diff
- TO '{ctx.aggs_dir}' (FORMAT parquet, PARTITION_BY (dt), APPEND)
+ TO '{ctx.aggs_dir}' (FORMAT parquet, PARTITION_BY (dt), OVERWRITE_OR_IGNORE)
```

The fix prevents the duplicate from being written in the first place, rather than removing it afterwards.

### 2. It knew the future: point-in-time correctness

> The ETA model's offline error looks excellent. In production it is noticeably worse.

`restaurant_prep_avg_30d` for date D is defined as the average prep time over the 30 days **before** D, not including D, because that is all the model can know when an order is placed on D.

**Cause.** The window frame in stage 3 ended at `CURRENT ROW`, so the partition for D included D's own deliveries. In training, a row's feature already contained the very day it was trying to predict, which made offline error look better than anything production could reach.

```diff
- RANGE BETWEEN INTERVAL {WINDOW_DAYS} DAY PRECEDING AND CURRENT ROW
+ RANGE BETWEEN INTERVAL {WINDOW_DAYS} DAY PRECEDING AND INTERVAL 1 DAY PRECEDING
```

R017 on 2026-03-08 now gives **33.0876**, the value that was actually knowable.

**The NULLs that appear are correct.** R208 made its first delivery on 2026-03-08, so on that date it has no history. A NULL says "unknown"; filling it with 0 or today's value would bring the leak back.

### 3. Nothing errored: a gate for silent upstream changes

> The orders team shipped a release on March 10. Nobody told us. The job succeeded, every gate passed, and the Sunday retrain used the data.

**Finding what changed.** I compared the input events of a normal day with March 10 using `make peek` (see the image at the top). The median `prep_time_minutes` went from **20.3 to 1218.0**, about 60x.

**Was it a real change?** No. If restaurants really took 60 times longer to cook, `delivery_minutes` would have gone up too, but it stayed at about 40. The only explanation consistent with both columns is that **upstream started sending seconds instead of minutes**.

**Why the existing gate missed it.** `rows_in_vs_trailing_7d` only counts rows. The row count was normal; the values inside the rows were not.

**The new gate: `medians_vs_trailing_7d`.** For each numeric input column (`cart_size`, `cart_value`, `prep_time_minutes`, `delivery_minutes`, `distance_km`), it compares today's median with the average of the previous 7 days' daily medians, and fires when any ratio leaves the band. It skips columns with no history yet, so the first days of the data stay quiet.

To choose the band, I first measured how much that ratio moves on normal days:

<p align="center"><img src="https://github.com/user-attachments/assets/89e665c0-81ae-4684-90ff-dd1edccfe14e" width="90%" alt="Ratio of today's median to the trailing 7-day average on normal days"></p>
<p align="center"><sub>On every normal day before March 10, the ratio stayed between <b>0.983 and 1.017</b></sub></p>

I set the band slightly wider, at **0.9 – 1.1**. It stays quiet on normal days, and because it checks every numeric column in both directions instead of being tuned to "prep time × 60", it also catches a different column changed by a different factor.

**What happens when each gate fires (`on_fail`):**

| Gate | `on_fail` | Why |
|---|---|---|
| `rows_in_vs_trailing_7d` (row count ±15%) | `quarantine` | A 15% swing in deliveries is worth checking, but not worth waking someone up at night. The partition goes to `quarantine/dt=D`, training never sees it, and the owner looks at it in the morning |
| `medians_vs_trailing_7d` (new) | `fail` | A unit change can move values by 60x or more, and if nobody notices it keeps poisoning the following days. Failing the run writes nothing for that date, keeps serving yesterday's features, and gets the owner talking to the upstream team before the next night's run |

On March 10 the new gate fires and stops the run:

```
medians_vs_trailing_7d fired: out of band: {'prep_time_minutes': 59.706}
run status on 2026-03-10: failed  (gate failed: medians_vs_trailing_7d)  outputs_written=[]
```

### 4. Yesterday was wrong: finding the damage and repairing it

> A bug in the feature job shipped on March 5 and was fixed on March 12. The wrong values are still on disk, and the weekly retrain ran while the bug was live.

The run manifests are the record of who wrote what. I loaded them into DuckDB and read them in the order they finished. `March 5 – 11` was **not** the answer, because three dates had been re-run:

| Date | What the manifests show | Verdict |
|---|---|---|
| 3/2 | Written by good code on 3/3, then **overwritten by the bad commit** on 3/6 | bad |
| 3/7 | First run failed and wrote nothing; the retry was the bad commit | bad |
| 3/9 | Written by the bad commit, then **re-run by fixed code** on 3/13 | already repaired |

**`queries/partition_status.sql`** gives one row per date with the run that wrote it **last** and whether that run was the bad commit:

```sql
unnest(outputs_written)                                     -- one row per partition a run wrote
WHERE output LIKE 'aggs/%'                                  -- a failed run wrote nothing, so it owns nothing
row_number() OVER (PARTITION BY dt ORDER BY finished_at DESC)  -- keep only the last writer
CASE WHEN git_sha = (SELECT bad_sha FROM incident) ...      -- bad SHA comes from the incident, never typed in
```

**Bad partitions: 3/2, 3/5, 3/6, 3/7, 3/8, 3/10, 3/11.** They are not contiguous, so I repaired them with three backfills of the normal pipeline (3/2, 3/5–3/8, 3/10–3/11). After the repair, every date is `ok`, and running the backfill a second time changes nothing, which is only true because Part 1 was fixed first.

**`queries/contaminated_models.sql`** follows each model's `inputs_read` to the exact run that produced each partition, and counts the ones written by the bad commit:

| Model | Trained | Bad partitions read |
|---|---|---|
| v43 | 3/8 | 4 |
| v44 | 3/15 | 7 |
| v45 | 3/22 | 3 |

v45 was trained on 3/22, ten days after the fix was deployed, and it still read three bad partitions. **Deploying the fix did not fix the data**; the old partitions stayed on disk until someone backfilled them.

This list does not change after the repair, because the models already learned from the old values. Repairing the table fixes what the next retrain reads; v43–v45 themselves have to be retrained on the repaired partitions.

### 5. What could still go wrong

**The job dies at partition 31.** A backfill over 40 dates is killed after writing 30. The table now holds 30 new partitions, 10 old ones, and possibly one half-written partition, and a model training at 3 a.m. learns from that mix without knowing it. The fix is to write the whole backfill to a temporary location and swap it in with a rename only once it is complete, so readers see either all old or all new.

**The next incident: values arrive as NULL.** If upstream starts sending NULL in a numeric column, both gates pass. The row-count gate still sees the rows, and the median gate computes its median over only the non-NULL values. Meanwhile the features built from those rows quietly shift. The check I would add is a **NULL-rate gate**: for each column, the fraction of today's rows that are NULL, compared against a threshold. Neither existing gate looks at that fraction, which is exactly why this failure gets through them.

<br>

## Result

```
PASS  part 0  happy path                   3/3 checks
PASS  part 1  it ran twice                 3/3 checks
PASS  part 2  it knew the future           3/3 checks
PASS  part 3  nothing errored              5/5 checks
PASS  part 4  yesterday was wrong          7/7 checks
5 of 5 parts pass
```

**What I took away:**
1. **A retry is only as safe as the code it re-runs.** Idempotency is not a nice-to-have in a batch job; it is what makes the retry loop and every backfill safe.
2. **"Nothing errored" is not "nothing is wrong."** Checks belong inside the pipeline, between stages, with the response to a failure decided before it happens.
3. **Fixing the code does not fix the data.** You need a record of which run wrote which partition, and which model read which run, to know what to repair and what to retrain.

<br>

## What I Changed

| File | What I did |
|---|---|
| [`src/build_features.py`](src/build_features.py) | Stage 4: `APPEND` → `OVERWRITE_OR_IGNORE` (Part 1). Stage 3: the window now ends the day before D (Part 2) |
| [`src/gates.py`](src/gates.py) | Added `medians_vs_trailing_7d`; chose `on_fail` for both gates (Part 3) |
| [`queries/partition_status.sql`](queries/partition_status.sql) | Which run wrote each date last, and whether it was the bad commit (Part 4) |
| [`queries/contaminated_models.sql`](queries/contaminated_models.sql) | Which model versions read bad partitions, and how many (Part 4) |

<br>

## Run It

```bash
docker compose build
docker compose run --rm app                         # opens the pipeline shell

make doctor                                         # should end with "all checks passed"
make data                                           # generate the synthetic events
make test                                           # run every part
make peek DATE=2026-03-10                           # input events and feature table for one date
make sql FILE=queries/partition_status.sql          # run a query against the run manifests
python -m src.runner --backfill 2026-03-05 2026-03-08
```

<br>

## Data

All order and delivery events are synthetic, generated by `src/generate_events.py` inside the container. Nothing in this repository comes from a real company or real customers.
