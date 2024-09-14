-- lucasmartinberardo_coderhouse.asteroids definition

-- Drop table

-- DROP TABLE lucasmartinberardo_coderhouse.asteroids;

--DROP TABLE lucasmartinberardo_coderhouse.asteroids;
CREATE TABLE IF NOT EXISTS lucasmartinberardo_coderhouse.asteroids
(
	date DATE NOT NULL  ENCODE az64
	,name VARCHAR(255)   ENCODE lzo
	,id INTEGER NOT NULL  ENCODE az64
	,close_approach_date DATE   ENCODE az64
	,estimated_diameter_min DOUBLE PRECISION   ENCODE RAW
	,estimated_diameter_max DOUBLE PRECISION   ENCODE RAW
	,kilometers_per_hour DOUBLE PRECISION   ENCODE RAW
	,kilometers DOUBLE PRECISION   ENCODE RAW
	,is_potentially_hazardous_asteroid BOOLEAN   ENCODE RAW
	,PRIMARY KEY (date, id)
)
DISTSTYLE AUTO
 DISTKEY (id)
;
ALTER TABLE lucasmartinberardo_coderhouse.asteroids owner to lucasmartinberardo_coderhouse;