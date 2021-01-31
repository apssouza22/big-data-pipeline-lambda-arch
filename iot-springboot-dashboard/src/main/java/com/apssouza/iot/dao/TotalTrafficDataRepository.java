package com.apssouza.iot.dao;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

import com.apssouza.iot.dao.entity.TotalTrafficData;

import java.util.UUID;

/**
 * DAO class for total_traffic 
 * 
 * @author abaghel
 *
 */
@Repository
public interface TotalTrafficDataRepository extends CassandraRepository<TotalTrafficData, UUID>{

	 @Query("SELECT * FROM traffickeyspace.total_traffic WHERE recorddate = ?0 ALLOW FILTERING")
	 Iterable<TotalTrafficData> findTrafficDataByDate(String date);
}
