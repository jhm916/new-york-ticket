/* move csv file to hdfs*/
hdfs dfs -put /home/Jihyun/NewYorkCombined.csv /NYC
/*access hive cli*/
beeline -u jdbc:hive2://localhost:10000/default -n Jihyun
/* create table schema for the csv file cutting the header and saving the file in hdfs under /output/*/
create table tickets
(summons_number int, plate_id string, registration_state string, plate_type string, issue_date date, violation_code int, vehicle_body_type string, vehicle_make string, issuing_agency string, street_code1 int, street_code2 int, street_code3 int,
vehicle_expiration_date int, violation_location string, violation_precinct int, issuer_precinct int, issuer_code int, issuer_command string, issuer_squad string, violation_time string, time_first_observed string, violation_county string,
violation_in_front_of_or_opposite string, house_number string, street_name string, intersecting_street string, date_first_observed int, law_section int, sub_division string, violation_legal_code string, days_parking_in_effect string,
from_hours_in_effect string, to_hours_in_effect string, vehicle_color string, unregistered_vehicle string, vehicle_year int, meter_number string, feet_from_curb int, violation_post_code string, violation_description string,
no_standing_or_stopping_violation string, hydrant_violation string, double_parking_violation string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/Output/"
TBLPROPERTIES("skip.header.line.count"="1");
/*Load data from the csv file onto the table made*/ /*csv file no longer in hdfs and will need to use -put again if needed*/
LOAD DATA INPATH '/NYC/NewYorkCombined.csv' OVERWRITE INTO TABLE tickets;
/* add a new column in the tickets table*/
ALTER TABLE tickets ADD COLUMNS (location_state string);	

/*second run to test concatenation

CREATE TABLE IF NOT EXISTS ticketsv2
(summons_number int, plate_id string, registration_state string, plate_type string, issue_date date, violation_code int, vehicle_body_type string, vehicle_make string, issuing_agency string, street_code1 int, street_code2 int, street_code3 int,
vehicle_expiration_date int, violation_location string, violation_precinct int, issuer_precinct int, issuer_code int, issuer_command string, issuer_squad string, violation_time string, time_first_observed string, violation_county string,
violation_in_front_of_or_opposite string, full_street string, location_state string, intersecting_street string, date_first_observed int, law_section int, sub_division string, violation_legal_code string, days_parking_in_effect string,
from_hours_in_effect string, to_hours_in_effect string, vehicle_color string, unregistered_vehicle string, vehicle_year int, meter_number string, feet_from_curb int, violation_post_code string, violation_description string,
no_standing_or_stopping_violation string, hydrant_violation string, double_parking_violation string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/Output/ticketsv2/";
//write data from first table while concatenating two columns and populating the new column with a case statement
INSERT OVERWRITE TABLE ticketsv2
SELECT summons_number, plate_id, registration_state, plate_type, issue_date, violation_code, vehicle_body_type, vehicle_make, issuing_agency, street_code1, street_code2, street_code3,
vehicle_expiration_date, violation_location, violation_precinct, issuer_precinct, issuer_Code, issuer_command, issuer_squad, violation_time, time_first_observed, violation_county,
violation_in_front_of_or_opposite, CONCAT(house_number,' ',street_name) AS full_street, case WHEN location_state is NOT NULL then 'New York' ELSE 'New York' END location_state, intersecting_street, date_first_observed, law_section, sub_division,
 violation_legal_code, days_parking_in_effect,from_hours_in_effect, to_hours_in_effect, vehicle_color, unregistered_vehicle, vehicle_year, meter_number, feet_from_curb, violation_post_code, violation_description,
no_standing_or_stopping_violation, hydrant_violation, double_parking_violation
FROM tickets;
*/

/* better version of tickets v2, concatenating 3 fields without creating a new column*/ 
CREATE TABLE IF NOT EXISTS ticketsfinal
(summons_number int, plate_id string, registration_state string, plate_type string, issue_date date, violation_code int, vehicle_body_type string, vehicle_make string, issuing_agency string, street_code1 int, street_code2 int, street_code3 int,
vehicle_expiration_date int, violation_location string, violation_precinct int, issuer_precinct int, issuer_code int, issuer_command string, issuer_squad string, violation_time string, time_first_observed string, violation_county string,
violation_in_front_of_or_opposite string, full_address string, intersecting_street string, date_first_observed int, law_section int, sub_division string, violation_legal_code string, days_parking_in_effect string,
from_hours_in_effect string, to_hours_in_effect string, vehicle_color string, unregistered_vehicle string, vehicle_year int, meter_number string, feet_from_curb int, violation_post_code string, violation_description string,
no_standing_or_stopping_violation string, hydrant_violation string, double_parking_violation string)
ROW FORMAT DELIMITED
STORED AS TEXTFILE
LOCATION "/Output/ticketsfinal/";

INSERT OVERWRITE TABLE ticketsfinal
SELECT summons_number, plate_id, registration_state, plate_type, issue_date, violation_code, vehicle_body_type, vehicle_make, issuing_agency, street_code1, street_code2, street_code3,
vehicle_expiration_date, violation_location, violation_precinct, issuer_precinct, issuer_Code, issuer_command, issuer_squad, violation_time, time_first_observed, violation_county,
violation_in_front_of_or_opposite, CONCAT(house_number,' ',street_name,',', case WHEN summons_number is NOT NULL then 'New York' ELSE 'New York' END) AS full_address, intersecting_street, date_first_observed, law_section, sub_division, violation_legal_code, days_parking_in_effect,from_hours_in_effect, to_hours_in_effect, vehicle_color, unregistered_vehicle, vehicle_year, meter_number, feet_from_curb, violation_post_code, violation_description,
no_standing_or_stopping_violation, hydrant_violation, double_parking_violation
FROM tickets;
/* working concat + case state

CREATE TABLE IF NOT EXISTS ticketstest
(summons_number int, registration_state string, full_street string, state string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE ticketstest
SELECT summons_number, registration_state, CONCAT(house_number,' ',street_name) AS full_street, case WHEN state is NOT NULL then 'New York' ELSE 'New York' END state
FROM tickets;

*/


