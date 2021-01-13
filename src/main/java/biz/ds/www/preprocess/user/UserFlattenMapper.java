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
package biz.ds.www.preprocess.user;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import biz.ds.www.types.UserWrTextType;
import biz.ds.www.types.UserWritable;

//user, server
//name, emailid, phonenumber, gender, city, sessionID- ipaddress, page, timestamp
//{sessionID, Timestamp}-{ipaddress, name, emailID, phoneNu, gender, city, maritalStatus, age, page, device, browser, paymentType time, date}
public class UserFlattenMapper extends Mapper<LongWritable, Text, UserWritable, Text>{

	private static final Log LOG = LogFactory.getLog(UserFlattenMapper.class);
	
	private UserFlattenMapperParser parser = new UserFlattenMapperParser();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(LOG.isDebugEnabled()){
			LOG.debug(UserFlattenMapper.class.getSimpleName()+": map()... Key:"+key);
		}
		
		UserWrTextType wrTextType = parser.parse(value);
		context.write(wrTextType.getUw(), wrTextType.getText());
	}
	
	

}


