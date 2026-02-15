SELECT id, start, "end" AS end, has_lunch
 FROM bpm.timetable_shifts
 WHERE id = ANY (%(ids)s:: int [])