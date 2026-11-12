USE olympic_dataset;

CREATE TABLE IF NOT EXISTS athlete_event_results (
    edition VARCHAR(255),
    edition_id INT,
    country_noc VARCHAR(10),
    sport VARCHAR(255),
    event_name VARCHAR(255),
    result_id INT,
    athlete VARCHAR(255),
    athlete_id INT,
    pos VARCHAR(20),
    medal VARCHAR(20),
    is_team_sport VARCHAR(10)
);

LOAD DATA INFILE '/var/lib/mysql-files/athlete_event_results.csv'
INTO TABLE athlete_event_results
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(edition, edition_id, country_noc, sport, event_name, result_id, athlete,
 athlete_id, pos, medal, is_team_sport);
