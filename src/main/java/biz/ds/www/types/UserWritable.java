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
package biz.ds.www.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UserWritable implements WritableComparable<UserWritable> {

	private Text sessionID;
	private Text timestamp;

	public UserWritable() {
		set(new Text(), new Text());
	}

	public UserWritable(String sessionID, String timestamp) {
		set(new Text(sessionID), new Text(timestamp));
	}

	public UserWritable(Text sessionID, Text timestamp) {
		set(sessionID, timestamp);
	}

	public void set(Text sessionID, Text timestamp) {
		this.sessionID = sessionID;
		this.timestamp = timestamp;
	}

	
	public Text getSessionID() {
		return sessionID;
	}

	public Text getTimestamp() {
		return timestamp;
	}

	@Override
	public void readFields(DataInput din) throws IOException {
		sessionID.readFields(din);
		timestamp.readFields(din);
	}

	@Override
	public void write(DataOutput dout) throws IOException {
		sessionID.write(dout);
		timestamp.write(dout);
	}

	
	@Override
	public int hashCode() {
		return sessionID.hashCode()*163 + timestamp.hashCode();
	}
	
	@Override
	public String toString() {
		return sessionID + "\t" + timestamp;
	}
	
	@Override
	public int compareTo(UserWritable uw) {
		int cmp = compare(sessionID,uw.sessionID); //sessionID.compareTo(uw.sessionID);
		
		if(cmp != 0){
			return cmp;
		}
		
		try{
			long meT = Long.parseLong(timestamp.toString());
			long cmpT = Long.parseLong(uw.timestamp.toString());
			return compare(new LongWritable(meT),new LongWritable(cmpT));//new LongWritable(meT).compareTo(new LongWritable(cmpT));
		}catch(Exception e){
			e.printStackTrace();
			throw new RuntimeException("Error in comparing long timestamp.",e);
		}
		
	}

	public static int compare(Text a , Text b){
		return a.compareTo(b);
	}
	
	public static int compare(LongWritable a , LongWritable b){
		return a.compareTo(b);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof UserWritable) {
			UserWritable tp = (UserWritable) o;
			return sessionID.equals(tp.sessionID) && timestamp.equals(tp.timestamp);
		}
		return false;
	}
}
