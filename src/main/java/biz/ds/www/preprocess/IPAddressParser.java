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

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

public class IPAddressParser {
	
	private static final Log LOG = LogFactory.getLog(IPAddressParser.class);	
	
	public String[] parse(Text value){
		return parse(value.toString());
	}
	
	// ipadress,date
	public String[] parse(String value){
		//IP Address is first in \t separated string value
		String ipaddress = value.substring(0,value.indexOf("\t")).trim();
		
		//Date is last in \t separated string value
		String date = value.substring(value.lastIndexOf("\t")+1).trim();
		
		if(LOG.isDebugEnabled()){
			LOG.debug("Parsed IP Address:"+ipaddress);
			LOG.debug("Parsed date:"+date);
		}
		String [] strs = {ipaddress,date};
		
		return strs;
	}
	
	public static void main(String[] args) {
		String value = "dinesh	sachdev	22:34:23	12-11-2012";
		System.out.println(Arrays.toString(new IPAddressParser().parse(value)));
	}
}