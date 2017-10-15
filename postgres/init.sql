CREATE DATABASE demo;

\connect demo

CREATE TABLE IF NOT EXISTS curve (
  curve_id bigserial PRIMARY KEY,
  name varchar(100) NOT NULL,
  source_id bigint DEFAULT NULL,
  last_update timestamp  DEFAULT NULL,
  meta varchar(500) DEFAULT NULL,
  UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS curve_data (
  curve_id bigint NOT NULL,
  value_date timestamp NOT NULL,
  value decimal NOT NULL,
  PRIMARY KEY (curve_id,value_date)
);

CREATE TABLE IF NOT EXISTS source (
  source_id bigserial PRIMARY KEY,
  source_name varchar(100) NOT NULL,
  script varchar(45) DEFAULT NULL,
  frequency varchar(1) DEFAULT NULL,
  parent_source_id bigint DEFAULT NULL,
  descr varchar(500) DEFAULT NULL,
  status varchar(45) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS debug_log (
  log_date timestamp NOT NULL,
  log_message VARCHAR(500) NULL
);

CREATE OR REPLACE function put_source_data(
	IN _source_name text, 
	IN _source_descr text, 
	IN _curve_meta text, 
	IN _curve_last_update timestamp, 
	IN _curve_ref text, 
	IN _curve_dates text, 
	IN _curve_values text)
RETURNS void AS $$
DECLARE 
     l_source_id integer;
     l_curve_id integer;
     l_curve_dates TEXT;
     l_curve_values TEXT;
     l_date_str TEXT;
     l_date timestamp;
     l_value TEXT;
BEGIN
        
    SELECT source_id INTO l_source_id FROM source WHERE source_name = _source_name;
    IF l_source_id IS NULL THEN 
		INSERT INTO source (source_name, descr) VALUES (_source_name, _source_descr )
		RETURNING source_id INTO l_source_id; 
    END IF;

	SELECT curve_id INTO l_curve_id FROM curve WHERE name = _curve_ref and source_id = l_source_id;
    IF l_curve_id IS NULL THEN 
		INSERT INTO curve (name, source_id, meta, last_update) VALUES (_curve_ref, l_source_id, _curve_meta, _curve_last_update)
		RETURNING curve_id INTO l_curve_id; 
 	END IF;
	l_curve_dates := _curve_dates || ',';
	l_curve_values := _curve_values || ',';

	WHILE (strpos(l_curve_values, ',') > 0)
	LOOP
		l_date_str := substr(l_curve_dates, 0, strpos(l_curve_dates, ','));
		l_date := to_timestamp(l_date_str,'YYYY-MM-DD');
		l_value = substr(l_curve_values, 0, strpos(l_curve_values, ','));
		l_curve_dates := substr(l_curve_dates, strpos(l_curve_dates, ',')+1);
		l_curve_values := substr(l_curve_values, strpos(l_curve_values, ',')+1);
        if l_date is not null and  l_value is not null then
		INSERT INTO curve_data VALUES(l_curve_id, l_date, cast(l_value as double precision))
			ON CONFLICT (curve_id, value_date) DO UPDATE set value = cast(l_value as double precision);
	    end if;
	
    END LOOP;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_last_update(
  p_name VARCHAR(255)
) RETURNS timestamp AS $$
DECLARE l_last_update timestamp;
BEGIN
SELECT max(last_update) INTO l_last_update 
FROM curve c, source s WHERE s.source_id = c.source_id and s.source_name = p_name;
RETURN l_last_update;
END;
$$ LANGUAGE plpgsql;