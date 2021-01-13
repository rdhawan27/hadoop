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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import biz.ds.www.types.UserWritable;

public class UserWritableKeyComparator extends WritableComparator {

	protected UserWritableKeyComparator() {
		super(UserWritable.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		//To group together same sessionID and arranging by timestamp asc 
		UserWritable a = (UserWritable) w1;
		UserWritable b = (UserWritable) w2;
		
		int cmp =  UserWritable.compare(a.getSessionID(), b.getSessionID());
		
		if(cmp!=0){
			return cmp;
		}
		
		try{
			long meT = Long.parseLong(a.getTimestamp().toString());
			long cmpT = Long.parseLong(b.getTimestamp().toString());
			return -(UserWritable.compare(new LongWritable(meT),new LongWritable(cmpT)));// reverse ordering
		}catch(Exception e){
			e.printStackTrace();
			throw new RuntimeException("Error in comparing long timestamp.",e);
		}
		
	}
}
