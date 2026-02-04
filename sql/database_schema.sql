/*
 File shows the create instructions used to create the wildfire db
*/

CREATE TABLE WildfireCause (
	cause_id INT PRIMARY KEY,
	cause_text TEXT NOT NULL
);


CREATE TABLE Wildfire (
	wildfire_id SERIAL PRIMARY KEY,
	state_id VARCHAR(2) NOT NULL,
	fire_name TEXT NOT NULL,
	containment_date DATE NOT NULL,
	report_date DATE NOT NULL,
	cause_id INT REFERENCES WildfireCause (cause_id)
);

CREATE TABLE WildfireLocation (
	wildfire_id INT REFERENCES Wildfire (wildfire_id),
	longitude DECIMAL(10, 7) NOT NULL,
	latitude DECIMAL(10, 7) NOT NULL,
	PRIMARY KEY(wildfire_id)
);

CREATE TABLE WildfireSizeClass (
	size_class VARCHAR(1) PRIMARY KEY,
	min_acreage DECIMAL(6,2) NOT NULL,
	max_acreage DECIMAL(9,2) NOT NULL
);

CREATE TABLE WildfireSize (
	wildfire_id INT REFERENCES Wildfire (wildfire_id),
	size_class VARCHAR(1) REFERENCES WildfireSizeClass (size_class),
	acreage DECIMAL(9,2),
	PRIMARY KEY(wildfire_id)
);