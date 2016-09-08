package com.pubnub.stormexamples.spout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.*;


public class PubNubSpout extends BaseRichSpout {
	private static final Logger LOGGER = LoggerFactory.getLogger(PubNubSpout.class);
	private static final long serialVersionUID = 5791105750638629484L;
	private SpoutOutputCollector collector;
	private PNConfiguration pnConfiguration;
	private PubNub pubNub;
	private List<String> channels;

	@Override
	public void nextTuple() {
		// As we are going to emit from callback, make nextTuple tick as dummy with some sleep
		try {
			Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			LOGGER.error("Error while sleeping between nextTuple", e);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void open(Map conf, TopologyContext ctx, SpoutOutputCollector collector) {
		this.collector = collector;
		this.pnConfiguration = getPNConfig(conf);
		this.pubNub = new PubNub(pnConfiguration);
		channels = (List<String>) conf.get("PubNubSpout.channels");
		LOGGER.info("Subscribed channels: {}", channels);
		this.pubNub.addListener(new MessageReceiveCallBack(this.collector));
		this.pubNub.subscribe().channels(channels).execute();
		LOGGER.info("Subscribed async");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}
	
	@Override
    public void close() {
		pubNub.unsubscribeAll();
		pubNub.stop();
    }

	@SuppressWarnings("rawtypes")
	protected PNConfiguration getPNConfig(Map conf) {
		PNConfiguration pnConfig = new PNConfiguration();
		pnConfig.setSubscribeKey((String) conf.get("PubNubSpout.subscriberKey"));
		pnConfig.setFilterExpression((String)conf.get("PubNubSpout.filterExpression"));
		return pnConfig;
	}

	static class MessageReceiveCallBack extends SubscribeCallback {
		SpoutOutputCollector collector;

		public MessageReceiveCallBack(SpoutOutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void status(PubNub pubnub, PNStatus status) {
			if(status.isError()) {
				LOGGER.warn("Request:{}, error because of:{}", status.getCategory(), status.getClientRequest());
			}
		}

		@Override
		public void message(PubNub pubnub, PNMessageResult message) {
			LOGGER.debug("emitting msg:{}", message.getMessage());
			this.collector.emit(new Values(message.getMessage()));
		}

		@Override
		public void presence(PubNub pubnub, PNPresenceEventResult presence) {
		}

	}
}
