-- AI-assisted: query drafted by AI; I derived the 7 bad dates by hand from the manifests first
WITH writes AS (
    SELECT dt, run_id, git_sha, finished_at, unnest(outputs_written) AS output
    FROM runs
),
ranked AS (
    SELECT dt, run_id, git_sha,
           row_number() OVER (PARTITION BY dt ORDER BY finished_at DESC) AS recency
    FROM writes
    WHERE output LIKE 'aggs/%'
)
SELECT dt, run_id, git_sha,
       CASE WHEN git_sha = (SELECT bad_sha FROM incident) THEN 'bad' ELSE 'ok' END AS status
FROM ranked
WHERE recency = 1
ORDER BY dt;