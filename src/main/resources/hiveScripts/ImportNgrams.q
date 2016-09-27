-- Creates table in S3 with the Google Books Ngrams and applies the sanitization
-- through a regex.
-- Needs ${ngramsTable}, ${ngramsLocation}, ${regex}

-- Importing data from S3 into new table on HDFS.
CREATE EXTERNAL TABLE raw_${ngramsTable} (
 gram string,
 year int,
 occurrences bigint,
 pages bigint,
 books bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS SEQUENCEFILE
LOCATION '${ngramsLocation}'
;

-- Creating table to store the normalized data.
CREATE TABLE normalized_${ngramsTable} (
 gram string,
 year int,
 occurrences bigint
)
;

-- Inserting sanitized ngrams.
INSERT OVERWRITE TABLE normalized_${ngramsTable}
SELECT
 gram,
 year,
 occurrences
FROM
 raw_${ngramsTable}
WHERE
 gram REGEXP "${regex}"
;
