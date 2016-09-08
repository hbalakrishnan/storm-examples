package com.pubnub.stormexamples.event;

import java.io.Serializable;

import com.pubnub.stormexamples.data.GeoLocation;
import com.pubnub.stormexamples.data.Item;

public class PurchasedItemEvent implements Serializable {
	private static final long serialVersionUID = 1L;
	private Item item;
	private GeoLocation geoLocation;
	private int qty;
	private long time;
	
	public PurchasedItemEvent() {}

	public PurchasedItemEvent(Item item, GeoLocation geoLocation, int qty, long time) {
		this.item = item;
		this.geoLocation = geoLocation;
		this.qty = qty;
		this.setTime(time);
	}

	public Item getItem() {
		return item;
	}

	public void setItem(Item item) {
		this.item = item;
	}

	public GeoLocation getGeoLocation() {
		return geoLocation;
	}

	public void setGeoLocation(GeoLocation geoLocation) {
		this.geoLocation = geoLocation;
	}

	public int getQty() {
		return qty;
	}

	public void setQty(int qty) {
		this.qty = qty;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String toString() {
		return "[item = " + this.getItem().getName() + ", geoLocation=" + this.getGeoLocation() + ",qty=" + this.qty
				+ ", time=" + this.time + "]";
	}

}
