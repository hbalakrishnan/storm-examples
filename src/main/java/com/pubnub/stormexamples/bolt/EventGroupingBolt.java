package com.pubnub.stormexamples.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pubnub.stormexamples.event.PurchasedItemEvent;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EventGroupingBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(EventGroupingBolt.class);
	private static final long serialVersionUID = -3100223992704360249L;
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		PurchasedItemEvent event = (PurchasedItemEvent) input.getValue(0);
		LOGGER.debug("emitting event category:{} with event:{}", event.getItem().getCategory().name(), event);
		this.collector.emit(new Values(event.getItem().getCategory().name(), event));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("category", "event"));
	}

}
