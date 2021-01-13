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
package biz.ds.www.utils;


import java.util.Random;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.simple.JSONObject;

import com.javadocmd.simplelatlng.LatLng;

public class JSONUtils {

	//Loggers
	private static final Logger debugLog = Logger.getLogger("debugLogger");
	private static final Logger jsonLog = Logger.getLogger("reportsLogger");
		
	public static final String NAME_PROPS = "NAME";
	public static final String EMAIL_PROPS = "EMAIL";
	public static final String TIMESTAMP_PROPS = "TIMESTAMPMS";
	public static final String LOCATION_PROPS = "LOCATION";
	public static final String PAGE_PROPS = "PAGE";
	public static final String LONGITUDE_PROPS = "LONGITUDE";
	public static final String LATITUDE_PROPS = "LATITUDE";
	
	public static final String[] ARR_PAGES = { "login.jsp", "signup.jsp",
			"index.jsp", "inobx.jsp", "groups.jsp", "profile.jsp",
			"success.jsp", "error.jsp" };
	public static final String[] DOMAIN_PAGES = { "test.com", "abc.biz" };

	public static void writeJSON() {

		debugLog.info( "writeJSON().. started...");
		
		JSONObject parent = new JSONObject();
		
		JSONObject child1 = new JSONObject();
		child1.put("LOCATION", "US");
		child1.put("LATITUDE", "123");
		child1.put("LONGITUDE", "4567");
		
		parent.put("ADDRESS", child1);
		
		System.out.println(parent);
		
		if(true)
			System.exit(1);
		
		JSONObject obj = new JSONObject();
		String uuid = UUID.randomUUID().toString();
		obj.put(NAME_PROPS, uuid);
		obj.put(EMAIL_PROPS, uuid.substring(0, 5) + "@gmail.com");
		obj.put(TIMESTAMP_PROPS, System.currentTimeMillis());

		Random random = new Random();
		char[] chs = { (char) ('a' + random.nextInt(26)),
				(char) ('a' + random.nextInt(26)) };

		obj.put(LOCATION_PROPS, new String(chs).toUpperCase());

		obj.put(PAGE_PROPS,
				"https://"
						+ DOMAIN_PAGES[new Random()
								.nextInt(DOMAIN_PAGES.length)] + '/'
						+ ARR_PAGES[new Random().nextInt(ARR_PAGES.length)]);

		LatLng latLng = LatLng.random();
		obj.put(LATITUDE_PROPS, latLng.getLatitude());
		obj.put(LONGITUDE_PROPS, latLng.getLongitude());

		/*
		 * try {
		 * 
		 * FileWriter file = new FileWriter("c:\\test.json");
		 * file.write(obj.toJSONString()); file.flush(); file.close();
		 * 
		 * } catch (IOException e) { e.printStackTrace(); }
		 */
		
		//debugLog.info( obj.toJSONString());
		//jsonLog.debug( obj.toJSONString());
		System.out.println(obj.toJSONString());
	}

	public static void main(String[] args) throws InterruptedException {

		PropertyConfigurator.configure(args[0]);
		while(true){
			writeJSON();
		//	Thread.sleep(new Random().nextInt(10)*1000);
		}
	}

}
