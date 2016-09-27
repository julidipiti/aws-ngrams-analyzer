-- Looks for the neologisms that appeared from one year to the other at shifting
-- the window.
-- Needs ${ngramsTable}, ${year}

-- Creating a table to store all the neologisms processed with this window size
-- and in the respective range.
CREATE TABLE IF NOT EXISTS pre_neologisms_${ngramsTable}
LIKE pre_dictionary_${ngramsTable}
;


-- Adding the neologisms of the year, which are the grams that were not present
-- in the previous window and appeared in at least ${percentOfYears} years.
INSERT INTO TABLE pre_neologisms_${ngramsTable}
SELECT *
FROM dictionary_${ngramsTable} as pdn
WHERE
 pdn.year=${year}
 AND NOT EXISTS (
  SELECT 1
  FROM dictionary_${ngramsTable} as dic
  WHERE
   dic.year=${year}-1
   AND
   dic.gram=pdn.gram
 )
;
 