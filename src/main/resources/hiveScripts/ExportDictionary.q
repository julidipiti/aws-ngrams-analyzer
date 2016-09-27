-- Creates a table in S3 with all the words of a language, according to the
-- percent of years a ngram is used.
-- Needs ${ngramsTable}, ${output}, ${windowSize}, ${percentOfYears}

-- Creating a table with all the words of a language.
CREATE EXTERNAL TABLE IF NOT EXISTS dictionary_${ngramsTable} (
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

-- Adding the words that appear at least ${percentOfYears}
INSERT OVERWRITE TABLE dictionary_${ngramsTable}
SELECT *
FROM pre_dictionary_${ngramsTable}
WHERE
 yearOccurrences>=FLOOR(${windowSize} * ${percentOfYears})
ORDER BY year, occurrences DESC
;
