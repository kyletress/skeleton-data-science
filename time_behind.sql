SELECT first_name, last_name, total_time - FIRST_VALUE(total_time)
OVER (PARTITION BY runs_count ORDER BY total_time ASC) AS time_behind_1,
total_time - NTH_VALUE(total_time, 3)
OVER (PARTITION BY runs_count ORDER BY total_time ASC) AS time_behind_3,
total_time - NTH_VALUE(total_time, 6)
OVER (PARTITION BY runs_count ORDER BY total_time ASC) AS time_behind_6,
rank()
OVER (ORDER BY status ASC, runs_count DESC, total_time ASC)
FROM (
	SELECT Entries.id, Entries.timesheet_id, Entries.athlete_id, Entries.status, Entries.bib, Entries.runs_count, sum(Runs.finish) 
	AS total_time 
	FROM Entries 
	LEFT JOIN Runs 
	ON (Entries.id = Runs.entry_id) 
	GROUP BY Entries.id
) AS FinalRanks
LEFT JOIN Athletes
ON (Athletes.id = FinalRanks.athlete_id)
WHERE timesheet_id = 29
ORDER BY status, rank, total_time ASC, bib ASC

/* US ATHLETES */
SELECT first_name, last_name, TRUNC(time_behind_1/100.0,2), TRUNC(time_behind_3/100.0,2), TRUNC(time_behind_6/100.0,2) 
FROM (
	SELECT first_name, last_name, country_code, total_time - FIRST_VALUE(total_time)
	OVER (PARTITION BY runs_count ORDER BY total_time ASC) AS time_behind_1,
	total_time - NTH_VALUE(total_time, 3)
	OVER (PARTITION BY runs_count ORDER BY total_time ASC) AS time_behind_3,
	total_time - NTH_VALUE(total_time, 6)
	OVER (PARTITION BY runs_count ORDER BY total_time ASC) AS time_behind_6,
	rank()
	OVER (ORDER BY status ASC, runs_count DESC, total_time ASC)
	FROM (
		SELECT Entries.id, Entries.timesheet_id, Entries.athlete_id, Entries.status, Entries.bib, Entries.runs_count, sum(Runs.finish) 
		AS total_time 
		FROM Entries 
		LEFT JOIN Runs 
		ON (Entries.id = Runs.entry_id) 
		GROUP BY Entries.id
	) AS FinalRanks
	LEFT JOIN Athletes
	ON (Athletes.id = FinalRanks.athlete_id)
	WHERE timesheet_id = 29
	ORDER BY status, rank, total_time ASC, bib ASC
	) AS usathletes
WHERE country_code = 'US'
