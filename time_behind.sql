SELECT CONCAT(Tracks.name,' ', Circuits.name), DATE(Timesheets.date), CONCAT(first_name,' ', last_name) AS athlete, TRUNC(time_behind_1/100.0,2) time_behind_1, TRUNC(time_behind_3/100.0,2) time_behind_3, TRUNC(time_behind_6/100.0,2) time_behind_6, placement 
FROM (
	SELECT first_name, last_name, country_code, timesheet_id, circuit_id, track_id,
	total_time - FIRST_VALUE(total_time) OVER (PARTITION BY timesheet_id, runs_count ORDER BY total_time ASC) time_behind_1,
	total_time - NTH_VALUE(total_time, 3) OVER (PARTITION BY timesheet_id, runs_count ORDER BY total_time ASC) time_behind_3,
	total_time - NTH_VALUE(total_time, 6) OVER (PARTITION BY timesheet_id, runs_count ORDER BY total_time ASC) time_behind_6,
	RANK() OVER (PARTITION BY timesheet_id ORDER BY FinalRanks.status ASC, runs_count DESC, total_time ASC) placement
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
	LEFT JOIN Timesheets
	ON (Timesheets.id = FinalRanks.timesheet_id)
	WHERE circuit_id = 1 AND race = true
	ORDER BY FinalRanks.status, placement, total_time ASC, bib ASC
	) AS usathletes
LEFT JOIN Circuits
ON (Circuits.id = usathletes.circuit_id)
LEFT JOIN Tracks
ON (Tracks.id = usathletes.track_id)
LEFT JOIN Timesheets
ON (Timesheets.id = usathletes.timesheet_id)
WHERE country_code = 'US'
ORDER BY timesheet_id ASC
