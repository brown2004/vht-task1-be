-- hotdata
CREATE TABLE IF NOT EXISTS aircraft (
    callsign VARCHAR(50) NOT NULL,
    detection_time BIGINT NOT NULL,
    category INT NOT NULL DEFAULT 0,
    mode_3a VARCHAR(10),
    classification VARCHAR(50),
    last_lat DOUBLE PRECISION NOT NULL,
    last_lng DOUBLE PRECISION NOT NULL,
    last_alt DOUBLE PRECISION NOT NULL,
    last_timestamp BIGINT NOT NULL, 
    PRIMARY KEY (callsign, detection_time)
);


CREATE TABLE IF NOT EXISTS history_position (
    id BIGSERIAL PRIMARY KEY, 
    aircraft_callsign VARCHAR(50) NOT NULL,
    aircraft_detection_time BIGINT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    alt DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION NOT NULL,   
    heading DOUBLE PRECISION NOT NULL, 
    timestamp BIGINT NOT NULL,  
    CONSTRAINT fk_aircraft
        FOREIGN KEY (aircraft_callsign, aircraft_detection_time)
        REFERENCES aircraft (callsign, detection_time)
        ON DELETE CASCADE 
);

CREATE INDEX IF NOT EXISTS idx_history_aircraft_time
ON history_position (aircraft_callsign, aircraft_detection_time, timestamp DESC);

-- cold data

CREATE TABLE IF NOT EXISTS archived_flight_summary (
    callsign VARCHAR(50) NOT NULL,
    detection_time BIGINT NOT NULL,
    category INT NOT NULL,
    classification VARCHAR(50),
    start_time BIGINT NOT NULL,
    end_time BIGINT NOT NULL, 
    PRIMARY KEY (callsign, detection_time)
);

CREATE TABLE IF NOT EXISTS archived_position (
    aircraft_callsign VARCHAR(50) NOT NULL,
    aircraft_detection_time BIGINT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    alt DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION NOT NULL,  
    heading DOUBLE PRECISION NOT NULL,  
    timestamp BIGINT NOT NULL,
    is_permanent BOOLEAN NOT NULL DEFAULT FALSE, 
    CONSTRAINT fk_archived_flight
        FOREIGN KEY (aircraft_callsign, aircraft_detection_time)
        REFERENCES archived_flight_summary (callsign, detection_time)
        ON DELETE CASCADE
);


CREATE INDEX IF NOT EXISTS idx_archived_position_cleanup 
ON archived_position (is_permanent, timestamp);


ALTER TABLE archived_position
ADD PRIMARY KEY (aircraft_callsign, aircraft_detection_time, timestamp);

-- note:
-- - dang thieu hai truong la last_speed, last_heading