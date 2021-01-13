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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import biz.ds.www.types.UserWrTextType;
import biz.ds.www.types.UserWritable;
import biz.ds.www.utils.json.ReaderJSON;
import biz.ds.www.utils.json.Server;
import biz.ds.www.utils.json.User;

class UserFlattenMapperParser{
	
	private static final Log LOG = LogFactory.getLog(UserFlattenMapperParser.class);
	
	private SimpleDateFormat datesdf = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat timesdf = new SimpleDateFormat("HH:mm:ss");

	public UserWrTextType parse(Text value){
		return parse(value.toString());
	}
	
	public UserWrTextType parse(String value){
		
		if(LOG.isDebugEnabled()){
			LOG.debug(UserFlattenMapperParser.class.getSimpleName()+": parse()...");
		}
		
		Server server = ReaderJSON.readJson(value);
		User user = server.getUser();
		
		UserWrTextType wrTextType = new UserWrTextType();
		wrTextType.setUw(new UserWritable(user.getSessionID(), server.getTimestamp()));
		
		Date d = new Date(Long.parseLong(server.getTimestamp()));
		
		StringBuffer buffer = new StringBuffer();
		buffer.append(server.getIpAddress()).append("\t")
		.append(user.getName()).append("\t")
		.append(user.getEmailID()).append("\t")
		.append(user.getPhoneNumber()).append("\t")
		.append(user.getGender()).append("\t")
		.append(user.getCity()).append("\t")
		.append(user.getMaritalStatus()).append("\t")
		.append(user.getAge()).append("\t")
		.append(server.getPage()).append("\t")
		.append(server.getDevice()).append("\t")
		.append(server.getBrowser()).append("\t")
		.append(server.getPaymentType()).append("\t")
		.append(timesdf.format(d)).append("\t")
		.append(datesdf.format(d));
		
		wrTextType.setText(new Text(buffer.toString()));
		
		if(LOG.isDebugEnabled()){
			LOG.debug("value parsed to create object of type UserWrTextType:\n"+wrTextType);
		}
		
		return wrTextType;
	}
}