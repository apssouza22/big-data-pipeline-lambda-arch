package com.apssouza.iot.dao;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

import com.apssouza.iot.dao.entity.WindowTrafficData;

import java.util.UUID;

/**
 * DAO class for window_traffic 
 * 
 * @author abaghel
 *
 */
@Repository
public interface WindowTrafficDataRepository extends CassandraRepository<WindowTrafficData,UUID>{
	
	@Query("SELECT * FROM traffickeyspace.window_traffic WHERE recorddate = ?0 ALLOW FILTERING")
	 Iterable<WindowTrafficData> findTrafficDataByDate(String date);

}
