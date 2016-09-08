package com.pubnub.stormexamples.data;

import java.io.Serializable;

public class Item implements Serializable {
	private static final long serialVersionUID = -4200902151909882494L;
	private int id;
	private String name;
	private Category category;
	
	public Item() {}
	
	public Item(int id, String name, Category category) {
		this.id = id;
		this.name = name;
		this.setCategory(category);
	}	

	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public Category getCategory() {
		return category;
	}

	public void setCategory(Category category) {
		this.category = category;
	}

	public static enum Category {
		ELECTRONICS, KITCHEN, CLOTHS, TOY, BEAUTY, OUTDOOR;
	}
}
