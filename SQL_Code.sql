-----------------------------------------------------
--SQL-Code Dunja Zoe Powroschnik
--Temperature indices
--22.01.2020
-----------------------------------------------------
--Create Views for each index
--1) TAVG

DROP view if exists temp_avg cascade;
CREATE VIEW temp_avg AS
SELECT
metadata.device_id as HOBO_ID,
avg(data.value) as avg_t
FROM data
Left JOIN metadata ON data.meta_id = metadata.id
WHERE metadata.term_id = 11
group by metadata.device_id
order by metadata.device_id ASC;
SELECT*FROM temp_avg;

--2) AVG daytime

DROP view if exists tempday cascade;
CREATE VIEW tempday AS
SELECT
metadata.device_id as HOBO_id,
avg(data.value) as avg_t_d
FROM data
Left JOIN metadata ON data.meta_id=metadata.id
WHERE metadata.term_id=11
AND EXTRACT(HOUR FROM data.tstamp) BETWEEN 6 and 18
group by metadata.device_id
order by metadata.device_id ASC;
SELECT*FROM tempday;

--3) AVG nighttime

DROP view if exists temp_night cascade;
CREATE VIEW temp_night AS
SELECT
metadata.device_id as HOBO_id,
avg(data.value) as avg_t_n
FROM data
Left JOIN metadata ON data.meta_id=metadata.id
WHERE metadata.term_id=11
AND (EXTRACT(HOUR FROM data.tstamp) <6
OR EXTRACT (HOUR FROM data.tstamp) >= 18)
group by metadata.device_id
order by metadata.device_id ASC;
SELECT*FROM temp_night;

--4) TND 
DROP view if exists temp_tnd cascade;
CREATE VIEW temp_tnd AS
SELECT
tempdays.hobo_id,
tempdays.avg_t_d,
tempnight.avg_t_n,
(tempdays.avg_t_d - tempnight.avg_t_n) as diff_tdn
FROM
tempdays
Left JOIN tempnight ON tempdays.hobo_id=tempnight.hobo_id;
SELECT*FROM temp_tnd;

--Create view with the before calculated indices

DROP view if exists indices_vw cascade;
CREATE VIEW indices_vw AS
SELECT
temp_tnd.hobo_id,
temp_tnd.avg_t_d,
temp_tnd.avg_t_n,
temp_tnd.diff_tdn,
temp_avg.avg_t
FROM
temp_tnd
Left JOIN temp_avg ON temp_tnd.hobo_id=temp_avg.hobo_id;
SELECT*FROM indices_vw;


--Cross-year indices
--Add neigbouring IDs and find the closest Hobos of WT19 and WT20
--for the hobos in WT21

DROP table if exists meta21 cascade;
create table meta21 as
SELECT *,
	(SELECT id FROM metadata ly WHERE term_id=9 ORDER BY st_distance(m.location, ly.location) ASC LIMIT 1) as close_meta20_id,
	(SELECT id FROM metadata ly WHERE term_id=7 ORDER BY st_distance(m.location, ly.location) ASC LIMIT 1) as close_meta19_id
	FROM metadata m
	WHERE term_id=11 AND sensor_id=1;
    ALTER Table meta21 ADD constraint pkey_meta21
    Primary key (id);
SELECT *
FROM meta21;

--create table with normalized temperature values

DROP VIEW IF EXISTS data_norm cascade;
CREATE VIEW data_norm AS
SELECT
	row_number() OVER (PARTITION BY meta_id ORDER BY tstamp ASC) as measurement_index,
	*,
	value - avg(value) OVER (PARTITION BY meta_id) AS norm,
	avg(value) OVER (PARTITION BY meta_id) AS group_avg
FROM data;
SELECT * FROM data_norm;

--calculate correlations Tcorr1Y and Tcorr2Y

Drop view if exists corr_table Cascade;
CREATE VIEW corr_table as
	SELECT
		meta21.id,
		avg(d.value) AS "mean",
		corr(d.norm, d20.norm) AS "corr1",
        corr(d.norm, d19.norm) AS "corr2"
	FROM data_norm AS d
	JOIN meta21 on meta21.id = d.meta_id
	JOIN metadata m20 on meta21.close_meta20_id=m20.id
    JOIN metadata m19 on meta21.close_meta19_id=m19.id
	--to be able to compare the measurements of the nearest HOBOs 
	--in the right order (pairs of first, second, third...measurement),
	--a measurement index had to be added, since 
	--the measurement periods of the different years are not uniform 
	--and the JOIN can not be made via the column tstamp (date).
	JOIN data_norm d20 on m20.id=d20.meta_id AND 
    d.measurement_index=d20.measurement_index
    JOIN data_norm d19 on m19.id=d19.meta_id AND 
    d.measurement_index=d19.measurement_index
	GROUP BY meta21.id;
SELECT*FROM corr_table;

--join corr_table with metadata

DROP TABLE IF Exists correlation_table cascade;
create table correlation_table as
select 
cor.id, "corr1", "corr2",
device_id, d.location, d.term_id
FROM corr_table cor
JOIN metadata d on d.id=cor.id;
Select * From correlation_table;

--Join cross-year indices (correlation_table) on indices_vw

DROP VIEW if exists indices_vw2 cascade;
CREATE VIEW indices_final AS
SELECT
i.hobo_id,
i.avg_t,
i.avg_t_d,
i.avg_t_n,
i.diff_tdn,
cor.corr1,
cor.corr2
FROM
indices_vw as i
JOIN correlation_table as cor ON i.hobo_id=cor.device_id;
SELECT*FROM indices_final

-----------------------------------------------------