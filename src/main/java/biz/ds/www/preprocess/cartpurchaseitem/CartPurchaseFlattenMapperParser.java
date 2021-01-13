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
package biz.ds.www.preprocess.cartpurchaseitem;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import biz.ds.www.types.DSACDCounter;
import biz.ds.www.types.UserWrTextType;
import biz.ds.www.types.UserWritable;
import biz.ds.www.utils.json.Item;
import biz.ds.www.utils.json.ReaderJSON;
import biz.ds.www.utils.json.Server;
import biz.ds.www.utils.json.User;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CartPurchaseFlattenMapperParser {

	private static final Log LOG = LogFactory.getLog(CartPurchaseFlattenMapperParser.class);
	
	private SimpleDateFormat datesdf = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat timesdf = new SimpleDateFormat("HH:mm:ss");
	private String type;
	private Context context;
		
	public CartPurchaseFlattenMapperParser(String type, Context context) {
		this.type = type;
		this.context = context;
	}

	
	public UserWrTextType[] parse(Text value){
		return parse(value.toString());
	}
	
	public UserWrTextType[] parse(String value){
		if(LOG.isDebugEnabled()){
			LOG.debug("parse()...");
		}
		
		Server server = ReaderJSON.readJson(value);
		Item[] items;
		
		if(LOG.isDebugEnabled()){
			LOG.debug("Array of items to fetch is from: "+type);
		}
		
		if(type.equals("cart")){
			items= server.getCartItems();
		}else{
			items= server.getPurchase();
		}
			
		User user = server.getUser();
		
		UserWrTextType[] wrTextTypes = new UserWrTextType[items.length];
		
		if(items.length==0){
			LOG.warn("Length of "+type+" items is 0. So, incremneting counter");
			
			if(type.equals("cart")){
				context.getCounter(DSACDCounter.NO_CART_ITEM).increment(1);
			}else{
				context.getCounter(DSACDCounter.NO_PURCHASE_ITEM).increment(1);
			}
			
			return wrTextTypes;
		}
		
		UserWritable uw = new UserWritable(user.getSessionID(), server.getTimestamp());
				
		Date d = new Date(Long.parseLong(server.getTimestamp()));
		
		int i=0;
		
		for(Item item:items){
			UserWrTextType wrTextType = new UserWrTextType();
			
			// Set same key each time instead of creating new as 
			// we are not going to modify the key object in 
			//future processing
			wrTextType.setUw(uw);
			
			StringBuffer buffer = new StringBuffer();
			buffer.append(server.getIpAddress()).append("\t")
			.append(item.getItemID()).append("\t")
			.append(item.getItemDesc()).append("\t")
			.append(item.getItemType()).append("\t")
			.append(item.getPrice()).append("\t")
			.append(timesdf.format(d)).append("\t")
			.append(datesdf.format(d));
			
			wrTextType.setText(new Text(buffer.toString()));
			
			if(LOG.isDebugEnabled()){
				LOG.debug("value parsed to create object of type UserWrTextType:\n"+wrTextType);
			}
			
			wrTextTypes[i] = wrTextType;
			i++;
		}
	
		return wrTextTypes;
	}
}
