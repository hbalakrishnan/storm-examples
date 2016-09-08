package com.pubnub.examples;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.PNCallback;
import com.pubnub.api.models.consumer.PNPublishResult;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.examples.config.ConfigReader;
import com.pubnub.stormexamples.data.GeoLocation;
import com.pubnub.stormexamples.data.Item;
import com.pubnub.stormexamples.data.Item.Category;
import com.pubnub.stormexamples.event.PurchasedItemEvent;

public class GlobalSalesSimulator {
	private static final Logger LOGGER = LoggerFactory.getLogger(GlobalSalesSimulator.class);
	
	static class PublishCallBack extends PNCallback<PNPublishResult> {
		@Override
		public void onResponse(PNPublishResult result, PNStatus status) {
			System.out.println("status"+status.getCategory());
			if(status.isError()) {
				LOGGER.warn("Retrying msg because of: {} ",status.getCategory());
				status.retry();
			}
		}
    	
    }
	
	public static void main(String[] args) throws Exception {
		final List<Item> items = readItems();
		Random randX = new Random();
		Random randY = new Random();
		Random randQ = new Random();
		Random randItems = new Random();		
		int nItems = items.size();
		PNConfiguration pnConfiguration = new PNConfiguration();
		Map<String,Object> configMap = ConfigReader.getConfig();
	    pnConfiguration.setPublishKey((String)configMap.get("publishKey"));
	    pnConfiguration.setSubscribeKey((String)configMap.get("subscriberKey"));
	    pnConfiguration.setSecretKey((String)configMap.get("secretKey"));
	    
	    PubNub pubNub = new PubNub(pnConfiguration);
	    PublishCallBack callback = new PublishCallBack();
		Thread t = new Thread( () -> {
			long start = System.currentTimeMillis()/1000;
			while(System.currentTimeMillis()/1000-start < 120) {
				try {
					Thread.sleep(100);
					GeoLocation geoLocation = new GeoLocation(randX.nextInt(101), randY.nextInt(101));
					PurchasedItemEvent pItem = new PurchasedItemEvent(items.get(randItems.nextInt(nItems)), geoLocation, randQ.nextInt(10)+1, System.currentTimeMillis()/1000);
					LOGGER.info("Publishing event : {}", pItem);
					Map<String,String> meta = new HashMap<String, String>();
					meta.put("category", pItem.getItem().getCategory().name());
					pubNub.publish().channel("items-event").meta(meta).message(pItem).async(callback);
				} catch (Exception e) {
					LOGGER.error("Error while publishing event", e);
				}				
			}
		});
		t.start();
		t.join();
		pubNub.stop();
	}
	
	private static List<Item> readItems() throws Exception {
		List<Item> items = new ArrayList<Item>();
		try(InputStream in = GlobalSalesSimulator.class.getResourceAsStream("/products.txt")) {
			InputStreamReader inStream = new InputStreamReader(in);
			BufferedReader bufferedReader = new BufferedReader(inStream);
			String line = null;
			while((line = bufferedReader.readLine()) != null) {
				String[] data = line.split(",");
				Item item = new Item(Integer.valueOf(data[0]), data[1].trim(), Category.valueOf(data[2].trim()));
				items.add(item);
			}
		}
		return items;
	}
}
