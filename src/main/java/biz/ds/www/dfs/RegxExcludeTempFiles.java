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
package biz.ds.www.dfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegxExcludeTempFiles implements PathFilter {

	private final String regex;

	public RegxExcludeTempFiles(String regex) {
		this.regex = regex;
	}

	@Override
	public boolean accept(Path path) {
		return ! path.toString().matches(regex);
	}

	public static void main(String[] args) {
		RegxExcludeTempFiles regxExcludeTempFiles = new RegxExcludeTempFiles("([^\\s]+(\\.(?i)(tmp))$)");	
		System.out.println(regxExcludeTempFiles.accept(new Path("hdfs://sandbox.hortonworks.com:8020/user/root/dsacd/FlumeData.1443813712883.tmp")));
	}
}
