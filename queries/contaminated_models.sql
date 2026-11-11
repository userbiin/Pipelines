-- AI-assisted: query drafted by AI; I checked each model's count by hand against the manifests
WITH reads AS (
    SELECT model_version, unnest(inputs_read) AS input
    FROM models
)
SELECT reads.model_version, count(*) AS bad_partitions
FROM reads
JOIN runs ON runs.run_id = reads.input.run_id
WHERE runs.git_sha = (SELECT bad_sha FROM incident)
GROUP BY reads.model_version
ORDER BY reads.model_version;