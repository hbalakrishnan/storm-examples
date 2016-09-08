package com.pubnub.stormexamples.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
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

public class CategoryStatsBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(CategoryStatsBolt.class);
	private static final long serialVersionUID = -3100223992704360249L;
	private OutputCollector collector;
	private Map<String,Integer> globalStatsMap;
	private Map<String,Map<String,Integer>> statsMapByMinute;
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm");
	private long start = 0L;
	private Integer statsInterval = null;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.globalStatsMap = new HashMap<String, Integer>();
		this.statsMapByMinute = new HashMap<String, Map<String,Integer>>();
		this.statsInterval = (Integer) stormConf.get("CategoryStatsBolt.interval");
		if(this.statsInterval == null) {
			this.statsInterval = 10;
		}
	}

	@Override
	public void execute(Tuple input) {
		if(start == 0L) {
			start = System.currentTimeMillis();
		}
		String category = (String) input.getValue(0);
		PurchasedItemEvent event = (PurchasedItemEvent) input.getValue(1);
		
		Integer count = globalStatsMap.get(category);
		if(count == null) {
			count = 0;
		}
		count += event.getQty();
		globalStatsMap.put(category, count);
		
		Map<String,Integer> minuteStats = statsMapByMinute.get(category);
		if(minuteStats == null) {
			minuteStats = new HashMap<String,Integer>();
		}
		Date d = new Date(event.getTime()*1000);
		String formattedDate = dateFormat.format(d);
		Integer minutesCount = minuteStats.get(formattedDate);
		if(minutesCount == null) {
			minutesCount = 0;
		}
		minutesCount += event.getQty();
		minuteStats.put(formattedDate, minutesCount);
		statsMapByMinute.put(category, minuteStats);
		long current = System.currentTimeMillis();
		// emit stats for every 10 seconds
		if(current - start >= this.statsInterval * 1000) {
			printStats();
			globalStatsMap.clear();
			statsMapByMinute.clear();
			start = current;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("stats"));
	}
	
	@Override
    public void cleanup() {
		printStats();
    }  
	
	private void printStats() {
		LOGGER.info("final stats map:{}", globalStatsMap);
		LOGGER.info("final statsMapByMinute map:{}", statsMapByMinute);
	}

}
