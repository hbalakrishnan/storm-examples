package com.pubnub.examples.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigReader {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigReader.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();
	@SuppressWarnings("unchecked")
	public static Map<String,Object> getConfig() {
		Map<String,Object> configMap = null;
		try(InputStream in = ConfigReader.class.getResourceAsStream("/config.json")) {
			configMap = (Map<String,Object>)MAPPER.readValue(in, Map.class);	
		} catch(IOException ioEx) {
			LOGGER.error("Error while loading config", ioEx);
		}
		return configMap;
	}
}
