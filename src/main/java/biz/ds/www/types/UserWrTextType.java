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

import org.apache.hadoop.io.Text;

public class UserWrTextType {

	UserWritable uw;

	Text text;

	public UserWrTextType() {

	}

	public UserWrTextType(UserWritable uw, Text text) {
		this.uw = uw;
		this.text = text;
	}

	public UserWritable getUw() {
		return uw;
	}

	public void setUw(UserWritable uw) {
		this.uw = uw;
	}

	public Text getText() {
		return text;
	}

	public void setText(Text text) {
		this.text = text;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		return buffer.append("UserWritable:")
		.append(uw)
		.append("\nText:")
		.append(text.toString())
		.toString();		
	}
}
