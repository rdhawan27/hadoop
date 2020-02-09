/*
 * Copyright [2015] [Dinesh Sachdev]
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  */
package biz.ds.www.utils.json;

import java.util.Arrays;

public class Server {

	private String ipAddress;
	private String page;
	private String timestamp;
	private String device;
	private String browser;
	private String paymentType;
	private User user;
	private Item visitedItems;
	private Item[] cartItems;
	private Item[] purchase;
	
	
	public Server(String ipAddress, String page, String timestamp) {
		this.ipAddress = ipAddress;
		this.page = page;
		this.timestamp = timestamp;
	}
	
	public Server() {
		
	}
	
	
	
	public String getDevice() {
		return device;
	}

	public void setDevice(String device) {
		this.device = device;
	}

	public String getBrowser() {
		return browser;
	}

	public void setBrowser(String browser) {
		this.browser = browser;
	}

	public String getPaymentType() {
		return paymentType;
	}

	public void setPaymentType(String paymentType) {
		this.paymentType = paymentType;
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public Item getVisitedItems() {
		return visitedItems;
	}

	public void setVisitedItems(Item visitedItems) {
		this.visitedItems = visitedItems;
	}

	public Item[] getCartItems() {
		return cartItems;
	}

	public void setCartItems(Item[] cartItems) {
		this.cartItems = cartItems;
	}

	public Item[] getPurchase() {
		return purchase;
	}

	public void setPurchase(Item[] purchase) {
		this.purchase = purchase;
	}

	public String getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getPage() {
		return page;
	}
	public void setPage(String page) {
		this.page = page;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		return buffer.append("{")
				.append(ipAddress).append("\t")
				.append(page).append("\t")
				.append(timestamp).append("\t")
				.append(device).append("\t")
				.append(browser).append("\t")
				.append(paymentType).append("\t")
				.append(user).append("\t")
				.append(visitedItems).append("\t")
				.append(Arrays.toString(cartItems)).append("\t")
				.append(Arrays.toString(purchase))
				.append("}").toString();
	}
	
}
