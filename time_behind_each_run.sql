SELECT event, date, athlete, position as run_number,
TRUNC(time_behind_1/100.0,2) time_behind_1, 
TRUNC(time_behind_3/100.0,2) time_behind_3, 
TRUNC(time_behind_6/100.0,2) time_behind_6,
rank
FROM (

SELECT *,
finish - FIRST_VALUE(finish) OVER(PARTITION BY timesheet_id, position ORDER BY finish asc) time_behind_1,
finish - NTH_VALUE(finish,3) OVER(PARTITION BY timesheet_id, position ORDER BY finish asc) time_behind_3,
finish - NTH_VALUE(finish,6) OVER(PARTITION BY timesheet_id, position ORDER BY finish asc) time_behind_6,
RANK() OVER (PARTITION BY timesheet_id, position ORDER BY finish ASC)
FROM (
	SELECT timesheet_id, CONCAT(Tracks.name, ' ', Circuits.name) as event, DATE(Timesheets.date), CONCAT(Athletes.first_name, ' ', Athletes.last_name) athlete, Athletes.country_code, Runs.finish, Runs.position
	FROM Runs
	LEFT JOIN Entries
	ON (Entries.id = Runs.entry_id)
	LEFT Join Timesheets
	ON (Timesheets.id = Entries.timesheet_id)
	LEFT JOIN Athletes
	ON (Athletes.id = Entries.athlete_id)
	LEFT JOIN Tracks
	ON (Tracks.id = Timesheets.track_id)
	LEFT JOIN Circuits
	ON (Circuits.id = Timesheets.circuit_id)
	WHERE Circuits.id = 1 AND Timesheets.race = TRUE
) AS RankedRuns
) AS FinalTable
WHERE country_code = 'US'
