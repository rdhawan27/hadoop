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

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import biz.ds.www.types.UserWrTextType;
import biz.ds.www.types.UserWritable;

//user- item
//{sessionID, Timestamp}-{ipaddress, itemID, itemDesc, itemType, price, time, date}
public class CartPurchaseFlattenMapper extends Mapper<LongWritable, Text, UserWritable, Text>{

	private static final Log LOG = LogFactory.getLog(CartPurchaseFlattenMapper.class);
	
	public static final String TYPE_CART_PURCH_PROP = "TYPE_CART_PURCH" ;
	
	private CartPurchaseFlattenMapperParser parser;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		String type = configuration.get(TYPE_CART_PURCH_PROP);
		LOG.info("Creating object parser of type:"+type);
		parser = new CartPurchaseFlattenMapperParser(type,context);
		
		super.setup(context);		
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(LOG.isDebugEnabled()){
			LOG.debug("map()... Key:"+key);
		}
		
		UserWrTextType[] wrTextTypes = parser.parse(value);
		
		for(UserWrTextType wrTextType: wrTextTypes){
			context.write(wrTextType.getUw(), wrTextType.getText());
		}
		
	}
}