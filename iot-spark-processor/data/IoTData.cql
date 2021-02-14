//Create keyspace
CREATE KEYSPACE IF NOT EXISTS TrafficKeySpace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

//Create table
CREATE TABLE TrafficKeySpace.Total_Traffic (routeId text ,  vehicleType text,  totalCount bigint, timeStamp timestamp,recordDate text,PRIMARY KEY (routeId,recordDate,vehicleType));
CREATE TABLE TrafficKeySpace.Window_Traffic (routeId text , vehicleType text,  totalCount bigint, timeStamp timestamp,recordDate text,PRIMARY KEY (routeId,recordDate,vehicleType));
CREATE TABLE TrafficKeySpace.Poi_Traffic(vehicleid text , vehicletype text , distance bigint,  timeStamp timestamp,PRIMARY KEY (vehicleid));
CREATE TABLE TrafficKeySpace.Heat_Map(latitude double , longitude double , totalcount bigint,  timeStamp timestamp,PRIMARY KEY (latitude, longitude));
CREATE TABLE TrafficKeySpace.Heat_Map_Batch(latitude double , longitude double , totalcount bigint,  timeStamp timestamp,PRIMARY KEY (latitude, longitude));
CREATE TABLE TrafficKeySpace.Poi_traffic_batch(vehicleid text , vehicletype text , distance bigint,  timeStamp timestamp,PRIMARY KEY (vehicleid));
CREATE TABLE TrafficKeySpace.Total_Traffic_batch (routeId text ,  vehicleType text,  totalCount bigint, timeStamp timestamp,recordDate text,PRIMARY KEY (routeId,recordDate,vehicleType));
CREATE TABLE TrafficKeySpace.Window_Traffic_batch (routeId text , vehicleType text,  totalCount bigint, timeStamp timestamp,recordDate text,PRIMARY KEY (routeId,recordDate,vehicleType));
