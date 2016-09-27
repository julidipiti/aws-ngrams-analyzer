-- Creates a table in S3 with N neologisms per year, ordered by occurrences.
-- Needs ${ngramsTable}, ${output}

CREATE EXTERNAL TABLE IF NOT EXISTS neologisms_${ngramsTable} (
 gram string,
 year int,
 occurrences bigint,
 yearoccurrences bigint
)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 LOCATION '${output}'
;

INSERT OVERWRITE TABLE neologisms_${ngramsTable}
SELECT gram, year, occurrences, yearOccurrences
FROM
 (
  SELECT
   *,
   rank() over (PARTITION BY sq.year ORDER BY sq.occurrences DESC) as rank
  FROM pre_neologisms_${ngramsTable} as sq
 ) sq_table
WHERE sq_table.rank <= 20
;
