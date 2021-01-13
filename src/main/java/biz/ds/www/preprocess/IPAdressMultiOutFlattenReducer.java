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
package biz.ds.www.preprocess;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import biz.ds.www.preprocess.IPAddressParser;
import biz.ds.www.types.UserWritable;

public class IPAdressMultiOutFlattenReducer extends Reducer<UserWritable, Text, UserWritable, Text> {

	private static final Log LOG = LogFactory.getLog(IPAdressMultiOutFlattenReducer.class);
	
	private MultipleOutputs<UserWritable, Text> multipleOutputs;
	
	private IPAddressParser parser = new IPAddressParser();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LOG.info("setup()...");
		
		multipleOutputs = new MultipleOutputs<UserWritable, Text>(context);
	}
	
	@Override
	protected void reduce(UserWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		//file name should start with IP Address
		for (Text value : values) {
			
			String[] strs = parser.parse(value);
			
			String basePath = String.format("%s/%s/part","ipaddress="+strs[0], "date="+strs[1]);
			
			multipleOutputs.write(key, value, basePath);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}

