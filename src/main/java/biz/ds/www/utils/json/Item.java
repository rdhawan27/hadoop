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

public class Item {

	private String itemID;
	private String itemDesc;
	private String itemType;
	private long price;
	
	
	public Item(String itemID, String itemDesc, String itemType) {
		this.itemID = itemID;
		this.itemDesc = itemDesc;
		this.itemType = itemType;
	}
	
	
	public Item(String itemID, String itemDesc, String itemType, long price) {
		this.itemID = itemID;
		this.itemDesc = itemDesc;
		this.itemType = itemType;
		this.price = price;
	}

	
	public Item() {
		
	}
	
	
	public long getPrice() {
		return price;
	}


	public void setPrice(long price) {
		this.price = price;
	}

	
	public String getItemID() {
		return itemID;
	}
	public void setItemID(String itemID) {
		this.itemID = itemID;
	}
	public String getItemDesc() {
		return itemDesc;
	}
	public void setItemDesc(String itemDesc) {
		this.itemDesc = itemDesc;
	}
	public String getItemType() {
		return itemType;
	}
	public void setItemType(String itemType) {
		this.itemType = itemType;
	}
	
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		return buffer.append("{")
				.append(itemID).append("\t")
				.append(itemDesc).append("\t")
				.append(itemType)
				.append("}").toString();
	}
}
