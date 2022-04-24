package com.apssouza.iot.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.apssouza.iot.common.dto.IoTData;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Class to deserialize JSON string to IoTData java object
 * 
 * @author abaghel
 *
 */
public class IoTDataDeserializer implements Deserializer<IoTData> {
	
	private static ObjectMapper objectMapper = new ObjectMapper();

	public IoTData fromBytes(byte[] bytes) {
		try {
			return objectMapper.readValue(bytes, IoTData.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void configure(Map<String, ?> map, boolean b) {

	}

	@Override
	public IoTData deserialize(String s, byte[] bytes) {
		return fromBytes((byte[]) bytes);
	}

	@Override
	public void close() {

	}
}
