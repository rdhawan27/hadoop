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
package biz.ds.www.utils.json;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.map.ObjectMapper;

public class ReaderJSON {

	public static Server readJson(String jsonStr){
		ObjectMapper mapper = new ObjectMapper();
		Server server = null;
		try{
			server  = mapper.readValue(jsonStr, Server.class);
		}catch(Exception e){
			e.printStackTrace();
		}
		return server;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String str = FileUtils.readFileToString(new File("C:\\test.json"));
		Server server = readJson(str);
		System.out.println(server);
		//System.out.println(server.getUser());
		//System.out.println(Arrays.toString(server.getPurchase()));
	}

}
