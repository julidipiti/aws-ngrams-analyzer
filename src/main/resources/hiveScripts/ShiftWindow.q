-- Shifts the window by 1 year, removing the oldest one and adding the next one 
-- in the window.
-- Needs ${ngramsTable}, ${newYear}, ${windowSize}
-- Adding all the ngrams in the specified year with their stats. We first
-- subtract the data to discount. 

-- Creating temporary table to store the data to subtract.
CREATE TEMPORARY TABLE tmp_old_year LIKE pre_dictionary_${ngramsTable}
;

-- Inserting the grams from the last year of our window to be subtracted of it.
INSERT OVERWRITE TABLE tmp_old_year
SELECT gram, year, occurrences, 1
FROM normalized_${ngramsTable}
WHERE year=${newYear}-${windowSize}
;

-- Creating temporary table to store the data to add up, with the same
-- structure.
CREATE TEMPORARY TABLE tmp_new_year LIKE pre_dictionary_${ngramsTable}
;

-- Inserting the grams from the year to add in our window.
INSERT OVERWRITE TABLE tmp_new_year
SELECT gram, year, occurrences, 1
FROM normalized_${ngramsTable}
WHERE year=${newYear}
;

-- Creating temporary table to store last year in range.
CREATE TEMPORARY TABLE tmp_last_year LIKE pre_dictionary_${ngramsTable}
;

-- Inserting a copy of the last year of the pre dictionary which has the stats
-- of all the window that ends in that year.
INSERT OVERWRITE TABLE tmp_last_year
SELECT *
FROM pre_dictionary_${ngramsTable}
WHERE year=${newYear}-1
;

-- Inserting a new year in the pre dictionary.
INSERT INTO TABLE pre_dictionary_${ngramsTable}
SELECT
 combined_table.sq_gram,
 ${newYear} as year,
 combined_table.sq_occurrences -
  COALESCE(toy.occurrences, 0) as occurrences,
 combined_table.sq_yearOccurrences -
  COALESCE(toy.yearOccurrences, 0) as yearOccurrences
FROM
 (
  SELECT
   COALESCE(tly.gram, tny.gram) as sq_gram,
   COALESCE(tly.occurrences, 0) +
    COALESCE(tny.occurrences, 0) as sq_occurrences,
   COALESCE(tly.yearOccurrences, 0) +
    COALESCE(tny.yearOccurrences, 0) as sq_yearOccurrences
  FROM tmp_last_year as tly
  FULL JOIN tmp_new_year tny ON tly.gram=tny.gram
 ) combined_table
LEFT JOIN tmp_old_year as toy ON combined_table.sq_gram=toy.gram
WHERE combined_table.sq_yearOccurrences - COALESCE(toy.yearOccurrences, 0) > 0
;

-- Deleting temporary table.
DROP TABLE tmp_old_year PURGE
;

-- Deleting temporary table.
DROP TABLE tmp_new_year PURGE
;

-- Deleting temporary table.
DROP TABLE tmp_last_year PURGE
;
