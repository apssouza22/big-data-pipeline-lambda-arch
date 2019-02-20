//Create keyspace
CREATE KEYSPACE IF NOT EXISTS TrafficKeySpace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

//Create table
CREATE TABLE TrafficKeySpace.Total_Traffic (routeId text ,  vehicleType text,  totalCount bigint, timeStamp timestamp,recordDate text,PRIMARY KEY (routeId,recordDate,vehicleType));
CREATE TABLE TrafficKeySpace.Window_Traffic (routeId text , vehicleType text,  totalCount bigint, timeStamp timestamp,recordDate text,PRIMARY KEY (routeId,recordDate,vehicleType));
CREATE TABLE TrafficKeySpace.Poi_Traffic(vehicleid text , vehicletype text , distance bigint,  timeStamp timestamp,PRIMARY KEY (vehicleid));

//Select from table
SELECT * FROM TrafficKeySpace.Total_Traffic;
SELECT * FROM TrafficKeySpace.Window_Traffic;
SELECT * FROM TrafficKeySpace.Poi_Traffic;

//Truncate table
TRUNCATE TABLE TrafficKeySpace.Total_Traffic;
TRUNCATE TABLE TrafficKeySpace.Window_Traffic;
TRUNCATE TABLE TrafficKeySpace.Poi_Traffic;