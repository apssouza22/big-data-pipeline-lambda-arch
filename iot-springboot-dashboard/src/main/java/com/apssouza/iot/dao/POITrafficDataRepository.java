package com.apssouza.iot.dao;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

import com.apssouza.iot.dao.entity.POITrafficData;

import java.util.UUID;

/**
 * DAO class for poi_traffic
 *
 */
@Repository
public interface POITrafficDataRepository extends CassandraRepository<POITrafficData,UUID>{
	 
}
