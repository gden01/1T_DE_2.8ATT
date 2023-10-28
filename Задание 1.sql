-- 1.1 Запрос, который формирует таблицы соответствующие по структуре и типу полей из входных csv-файлов
CREATE TABLE austin_bikeshare_trips (
    bikeid NUMERIC(9, 1), 
    checkout_time TIME,
    duration_minutes INT,
    end_station_id NUMERIC(9, 1), 
    end_station_name VARCHAR(255),
    month NUMERIC(9, 1), 
    start_station_id NUMERIC(9, 1), 
    start_station_name VARCHAR(255),
    start_time TIMESTAMP,
    subscriber_type VARCHAR(255),
    trip_id BIGINT, 
    year NUMERIC(9, 1) 
);

CREATE TABLE austin_bikeshare_stations (
    latitude NUMERIC(9, 5),
    location TEXT, 
    longitude NUMERIC(9, 5),
    name VARCHAR(255),
    station_id INT,
    status VARCHAR(255)
);

-- 1.2 Наполнение таблиц данными из файлов
COPY austin_bikeshare_trips FROM 'D:/Data/austin_bikeshare_trips.csv' DELIMITER ',' CSV HEADER;
COPY austin_bikeshare_stations FROM 'D:/Data/austin_bikeshare_stations.csv' DELIMITER ',' CSV HEADER QUOTE '"';

-- 1.3, 1.4 Запрос, который формирует таблицы для каждого года(с 2013 по 2017)
DO $$ 
DECLARE 
    year INT;
BEGIN
    FOR year IN 2013..2017 LOOP
        EXECUTE '
        CREATE TABLE station_data_' || year || ' AS
        SELECT
            t.start_station_id AS station_id,
            COUNT(*) FILTER (WHERE s.status = ''active'') AS trips_started,
            0 AS trips_ended,
            COUNT(*) FILTER (WHERE s.status = ''active'') AS total_trips,
            AVG(duration_minutes) FILTER (WHERE s.status = ''active'') AS avg_duration_started,
            0 AS avg_duration_ended
        FROM austin_bikeshare_trips t
        LEFT JOIN austin_bikeshare_stations s ON t.start_station_id = s.station_id
        WHERE EXTRACT(YEAR FROM t.start_time) = ' || year || '
        GROUP BY t.start_station_id
        HAVING COUNT(*) FILTER (WHERE s.status = ''active'') > 0
        ORDER BY AVG(duration_minutes) FILTER (WHERE s.status = ''active'');
        ';
    END LOOP;
END $$;

-- Запрос, который выводит названия 10 станций с самым высоким показателем средней продолжительности начавшихся поездок за 2016 год

SELECT
    s.station_id,
    s.name,
    sd.avg_duration_started
FROM austin_bikeshare_stations s
JOIN (
    SELECT
        t.start_station_id AS station_id,
        AVG(t.duration_minutes) AS avg_duration_started
    FROM austin_bikeshare_trips t
    JOIN station_data_2016 sd ON t.start_station_id = sd.station_id
    WHERE sd.trips_started > 0
    GROUP BY t.start_station_id
) AS sd ON s.station_id = sd.station_id
ORDER BY sd.avg_duration_started DESC
LIMIT 10;
