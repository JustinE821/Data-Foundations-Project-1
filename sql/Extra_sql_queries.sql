SELECT *
FROM wildfire w
INNER JOIN wildfirelocation wl ON w.wildfire_id=wl.wildfire_id
INNER JOIN wildfirecause wc ON w.cause_id=wc.cause_id
WHERE w.cause_id != 11
ORDER BY w.wildfire_id ASC;

--This could be helpful as the query for making the heat map
SELECT w.fire_name, w.state_id, wl.longitude, wl.latitude
FROM wildfire w
INNER JOIN wildfirelocation wl ON w.wildfire_id=wl.wildfire_id;


--Could be helpful in showing fire prevalence in certain states
SELECT state_id, COUNT(state_id) 
FROM wildfire
GROUP BY state_id
ORDER BY COUNT(state_id) DESC;

--Can be used to show the amount in which certain fire types occur in individual states
SELECT w.state_id, wc.cause_text, COUNT(w.state_id), ROW_NUMBER() OVER (PARTITION BY w.state_id ORDER BY COUNT(w.state_id) DESC) AS row_num 
FROM wildfire w
INNER JOIN wildfirecause wc ON w.cause_id=wc.cause_id
WHERE row_num = 1
GROUP BY w.state_id, wc.cause_text
ORDER BY w.state_id, COUNT(w.state_id) DESC;


--Shows the top causes of fires by state
WITH ranked_causes AS (
  SELECT 
    w.state_id,
    wc.cause_text,
   	COUNT(w.state_id) AS num_of_occurances,
    ROW_NUMBER() OVER (PARTITION BY w.state_id ORDER BY COUNT(w.state_id) DESC) AS row_num
  FROM wildfire w
  INNER JOIN wildfiresize ws ON w.wildfire_id = ws.wildfire_id
  INNER JOIN wildfirecause wc ON w.cause_id = wc.cause_id
  WHERE w.cause_id != 11
  GROUP BY w.state_id, wc.cause_text
)
SELECT state_id, cause_text, num_of_occurances
FROM ranked_causes
WHERE row_num = 1
ORDER BY state_id;


WITH ranked_causes AS (
  SELECT 
    w.state_id,
    wc.cause_text,
   	COUNT(w.state_id) AS num_of_occurances,
    ROW_NUMBER() OVER (PARTITION BY w.state_id ORDER BY COUNT(w.state_id) DESC) AS row_num
  FROM wildfire w
  INNER JOIN wildfiresize ws ON w.wildfire_id = ws.wildfire_id
  INNER JOIN wildfirecause wc ON w.cause_id = wc.cause_id
  WHERE w.cause_id != 11
  GROUP BY w.state_id, wc.cause_text
)
SELECT cause_text, COUNT(cause_text)
FROM ranked_causes
WHERE row_num = 1
GROUP BY cause_text;

--Can be used to find how common fire causes are
SELECT w.cause_id, wc.cause_text, COUNT(w.cause_id)
FROM wildfire w
INNER JOIN wildfirecause wc ON w.cause_id=wc.cause_id
GROUP BY wc.cause_text, w.cause_id
HAVING w.cause_
ORDER BY COUNT(w.cause_id) DESC;

SELECT wc.cause_text, COUNT(w.cause_id)
FROM wildfire w
INNER JOIN wildfirecause wc ON w.cause_id = wc.cause_id
GROUP BY wc.cause_text
ORDER BY COUNT(w.cause_id) DESC
LIMIT 5;

SELECT wc.cause_text, COUNT(w.cause_id)
FROM wildfire w
INNER JOIN wildfirecause wc ON w.cause_id = wc.cause_id
INNER JOIN wildfiresize ws ON w.wildfire_id = ws.wildfire_id
WHERE ws.acreage > 5
GROUP BY wc.cause_text
ORDER BY COUNT(w.cause_id) DESC
LIMIT 5;

SELECT COUNT(w.wildfire_id) 
FROM wildfire w
INNER JOIN wildfiresize ws ON w.wildfire_id = ws.wildfire_id
WHERE ws.acreage > 5;




