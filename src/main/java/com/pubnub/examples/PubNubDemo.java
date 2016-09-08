package com.pubnub.examples;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import com.pubnub.examples.config.ConfigReader;
import com.pubnub.stormexamples.bolt.CategoryStatsBolt;
import com.pubnub.stormexamples.bolt.EventGroupingBolt;
import com.pubnub.stormexamples.bolt.TypeConverterBolt;
import com.pubnub.stormexamples.spout.PubNubSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class PubNubDemo {
	
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		// pubnub spout to emit data from pubnub channels
		PubNubSpout pubNubSpout = new PubNubSpout();
		builder.setSpout("spout", pubNubSpout, 1);
		
		// convert jsonNode to the relevantObject and this is a reusable bolt so that anyone consumes from spout can use this bolt to convert into required type
		builder.setBolt("converter", new TypeConverterBolt(), 2).shuffleGrouping("spout");
		// group the events by category, grouping is mandatory to maintain stats from executor - grouping is required because same category events go to the same executer in downstream
		builder.setBolt("category-grouper", new EventGroupingBolt(), 2).shuffleGrouping("converter");
		// category stats bolt , use fields grouping here to get the same category in same execeuter
		builder.setBolt("category-stats", new CategoryStatsBolt(), 2).fieldsGrouping("category-grouper", new Fields("category"));
		
		
		// read config.json for keys
		Map<String,Object> configMap = ConfigReader.getConfig();
		
		Config conf = new Config();
		conf.put("PubNubSpout.subscriberKey", configMap.get("subscriberKey"));
		// use the same channel as global-sales-simulator
		conf.put("PubNubSpout.channels", Arrays.asList("items-event"));
		// filter the category of items that we are interested
		conf.put("PubNubSpout.filterExpression", "category LIKE '*'");
		// config for TypeConverterBolt to convert JsonNode to com.pubnub.stormexamples.event.PurchasedItemEvent
		conf.put("TypeConverterBolt.typeReferernceClass", "com.pubnub.stormexamples.event.PurchasedItemEvent");
		
		// set the parallelism
		conf.setMaxTaskParallelism(3);

		LocalCluster cluster = new LocalCluster();
		// build the topology and run in local code
		cluster.submitTopology("item-category-stats", conf, builder.createTopology());
		
		// run for 2 minutes
		Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MINUTES);
		pubNubSpout.deactivate();
		// allow 10 seconds after deactivating spout, so that all the inflight msgs are consumed
		Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
		// finally kill the cluster
		cluster.shutdown();
		
	}
}
