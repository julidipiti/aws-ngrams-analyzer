-- Creates a table in S3 with 
-- Needs ${ngramsTable1}, ${ngramsTable2}, ${output}

-- Creating a table for the foreignisms of a language.
CREATE EXTERNAL TABLE IF NOT EXISTS foreignisms_${ngramsTable1}_${ngramsTable2} (
 gram string,
 occurrences bigint
)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 LOCATION '${output}'
;

-- Inserts the foreignisms that the first language has from the latter. Basing 
-- the selection on the words that have a greater usage (N times more) on the
-- same gram.
INSERT OVERWRITE TABLE foreignisms_${ngramsTable1}_${ngramsTable2}
SELECT
 dic1.gram,
 SUM(dic1.occurrences) as tot
FROM dictionary_${ngramsTable1} as dic1
JOIN dictionary_${ngramsTable2} as dic2 
 ON dic1.gram=dic2.gram
WHERE
 dic1.occurrences*1000<dic2.occurrences
GROUP BY
 dic1.gram
ORDER BY
 tot DESC
LIMIT 1000
;
