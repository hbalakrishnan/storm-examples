package com.pubnub.stormexamples.data;

import java.io.Serializable;

/**
 * Location of an Item Purchased
 */
public class GeoLocation implements Serializable {

	private static final long serialVersionUID = -980544534474055804L;
	private int x;
	private int y;
	
	public GeoLocation() {}

	public GeoLocation(int x, int y) {
		this.x = x;
		this.y = y;
	}

	public int getX() {
		return this.x;
	}

	public int getY() {
		return this.y;
	}

	public String toString() {
		return "[GeoLocation: " + this.x + ", " + this.y + "]";
	}
}
