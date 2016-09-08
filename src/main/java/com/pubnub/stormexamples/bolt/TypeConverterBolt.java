package com.pubnub.stormexamples.bolt;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TypeConverterBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(TypeConverterBolt.class);
	private static final long serialVersionUID = -3100223992704360249L;
	private static ObjectMapper MAPPER;
	private Class<?> typeReferernceClass;
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		MAPPER = new ObjectMapper();
		String typeReferernce = (String) stormConf.get("TypeConverterBolt.typeReferernceClass");
		LOGGER.info("typeReferernceClass:{}", typeReferernce);
		try {
			typeReferernceClass = Class.forName(typeReferernce);
		} catch (ClassNotFoundException e) {
			LOGGER.error("Error while converting type reference:{}", typeReferernce, e);
			throw new RuntimeException("Cannot load class: " + typeReferernce, e);
		}
	}

	@Override
	public void execute(Tuple input) {
		JsonNode node = (JsonNode) input.getValue(0);
		Object obj = null;
		try {
			obj = MAPPER.readValue(node.toString(), typeReferernceClass);
			LOGGER.debug("Emitted object:{}", obj);
			this.collector.emit(new Values(obj));
		} catch (IOException e) {
			LOGGER.error("Error while emitting transformed object:{}", node, e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transformedMsg"));
	}

}
