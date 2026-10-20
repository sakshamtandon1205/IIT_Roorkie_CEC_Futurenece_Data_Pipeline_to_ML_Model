-- RAW layer: an exact copy of one CSV file, every column as text, nothing cleaned.
-- Extra columns: _loaded_at, _source_file, _run_id, _row_num (line number inside the file).
-- Raw tables only ever grow: a rerun with a new run_id appends another copy and nothing is deleted.
-- If the same run_id loads the same file twice (a task retry), the second attempt adds nothing.
CREATE TABLE IF NOT EXISTS {{table}} AS
SELECT *, TIMESTAMP '{{now}}' AS _loaded_at, '{{source_file}}' AS _source_file,
       '{{run_id}}' AS _run_id, CAST(0 AS BIGINT) AS _row_num
FROM read_csv('{{path}}', header = true, all_varchar = true, nullstr = '<<never-null>>')
LIMIT 0;

INSERT INTO {{table}}
SELECT *, TIMESTAMP '{{now}}', '{{source_file}}', '{{run_id}}', ROW_NUMBER() OVER ()
FROM read_csv('{{path}}', header = true, all_varchar = true, nullstr = '<<never-null>>')
WHERE NOT EXISTS (SELECT 1 FROM {{table}} WHERE _run_id = '{{run_id}}' AND _source_file = '{{source_file}}');
