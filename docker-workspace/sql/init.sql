CREATE DATABASE test_fligoo;

CREATE TABLE test_fligoo.testdata(
    flight_id int AUTO_INCREMENT,
    flight_date date,
    flight_status varchar(200),
    departure_airport varchar(200),
    departure_timezone varchar(200),
    arrival_airport varchar(200),
    arrival_timezone varchar(200),
    arrival_terminal varchar(200),
    airline_name varchar(200),
    flight_number int,
    created_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
    primary key (flight_id) 
);