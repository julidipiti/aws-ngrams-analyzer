-- Creates the window to analyze the ngrams.
-- Needs ${ngramsTable}, ${fromYear}, ${toYear}

-- Creating a pre dictionary for all the words of the language.
CREATE TABLE pre_dictionary_${ngramsTable} (
 gram string,
 year int,
 occurrences bigint,
 yearOccurrences int
)
;

-- Inserting all the ngrams within the range with their stats. All the ngrams
-- are considered to be in the last year of the range, even if they may not
-- appear actually in that year.
INSERT OVERWRITE TABLE pre_dictionary_${ngramsTable} 
SELECT t2.gram, ${toYear}-1, t2.totalOccurrences, t2.yearOccurrences
FROM
(
 -- Subquery to sum up all the year occurrences and total occurrences of a
 -- ngram, grouping them.
 SELECT
  t1.gram as gram,
  count(DISTINCT t1.year) as yearOccurrences,
  SUM(t1.occurrences) as totalOccurrences
 FROM
  (
   -- Subquery to filter all the ngrams within the range under observation.
   SELECT
    gram, year, occurrences
   FROM
    normalized_${ngramsTable}
   WHERE
    year>=${fromYear} AND year<${toYear}
  ) t1
 GROUP BY
   gram
) t2
;
