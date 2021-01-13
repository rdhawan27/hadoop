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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.codehaus.jackson.map.ObjectMapper;

public class WriterJSON {

	public static final String [] SERVER_LIST={"192.168.56.77","192.78.23.56","172.168.56.23","10.29.30.1","209.23.125.62",
		"209.23.16.47","192.168.204.16","192.168.64.39","192.168.190.128","192.168.142.123","172.56.89.50","172.83.49.52"};
	
	public static final String[] PAGES_LIST = { "login.jsp", "signup.jsp","index.jsp", "inobx.jsp", "groups.jsp", "profile.jsp","success.jsp", "error.jsp", "viewItem.jsp",
		"addCart.jsp","deletcart.jsp","payment.jsp","save.jsp","viewGraph.jsp","logout.jsp","redirect.jsp","node.jsp"};
	
	public static final String[] INTRESTS_LIST ={"Vechile", "Grossary", "Mobile", "Clothing", "Jwellery", "Food", "Recharge", "BusTicketBooking", "AirPlaneTicketBooking",
		"TrainTicketBooking", "BillPaymentElectricity", "BillPaymenWater", "Banking", "HousingLandProperty", "Books", "Toys", "Antiques", "Watches", "Eyewares"};
	
	public static final String[] MARITAL_STATUS = {"SINGLE", "MARRIED", "UNKNOWN"};
	
	public static final String[] DEVICE_LIST = {"MOBILE", "TABLET", "LAPTOP", "DESKTOP"};
	
	public static final String[] BROWSER_LIST = {"CHROME", "MOZILLA", "OPERA", "IE"};
	
	public static final String[] PAYMENT_LIST = {"NETBANKING", "MASTERCREDITCARD", "VISACREDITCARD", "DEBITCARD", "AMERICANEXPRESSCARD"};
	
	public static final int DEFAULT_MAX_AGE = 100;
	
	
	//Loggers
	private static final Logger debugLog = Logger.getLogger("debugLogger");
	private static final Logger jsonLog = Logger.getLogger("reportsLogger");
	
	//args[0] - Log4j properties
	//args[1] - number of threads
	//args[2] - sleep time in seconds
	public static void main(String[] args) throws Exception{
		
		PropertyConfigurator.configure(args[0]);
		
		Thread [] threads = new InternalThread[Integer.parseInt(args[1])];
		
		for(int i=0;i<threads.length;i++){
			threads[i] = new InternalThread(i%2==0?"M":"F", Integer.parseInt(args[2]));
		}
		for(int i=0;i<threads.length;i++){
			threads[i].start();
		}
	}
	
		
	static class InternalThread extends Thread{
		
		private SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy-HHa");
		
		private String gender;
		
		private int sleepSeconds;
		
		private Random random;
		
		public InternalThread(String gender, int sleepSeconds) {
			this.gender = gender;
			this.sleepSeconds= sleepSeconds;
			random = new Random();
		}

		@Override
		public void run() {
		while(true){
				Server server = getServer();
				ObjectMapper mapper = new ObjectMapper();
				try {
					String str = mapper.writeValueAsString(server);
					jsonLog.debug(str);
					//System.out.println(str);
				} catch (Exception e) {
					debugLog.error("Exception occured in writing json", e);
				}
			
				// Random sleep between 1 to sleepSeconds exclusive
				try {
					int sleepRand = random.nextInt(sleepSeconds);
					Thread.sleep((sleepRand==0?1:sleepRand)*1000);
				} catch (InterruptedException e) {
					debugLog.warn("Exception occured sleeping.\n"+e.getMessage());
				}
			}
		}
		
		private Server getServer(){
			int i = random.nextInt(SERVER_LIST.length);
			int j = random.nextInt(PAGES_LIST.length);
			
			Server server = new Server(SERVER_LIST[i], PAGES_LIST[j], Long.toString(System.currentTimeMillis()));
			
			int k = random.nextInt(DEVICE_LIST.length);
			int l = random.nextInt(BROWSER_LIST.length);
			int m = random.nextInt(PAYMENT_LIST.length);
			
			server.setDevice(DEVICE_LIST[k]);
			server.setBrowser(BROWSER_LIST[l]);
			server.setPaymentType(PAYMENT_LIST[m]);
			
			server.setUser(getUser());
			server.setCartItems(getCartPurchasedItem());
			server.setPurchase(getCartPurchasedItem());
			server.setVisitedItems(getVisitedItem());
			return server;
		}
		
		private Item getVisitedItem(){
			Item item = new Item();
			String uuid = UUID.randomUUID().toString();
			item.setItemID(uuid.substring(0,5));
			item.setItemDesc(uuid);
			int idx = random.nextInt(INTRESTS_LIST.length);
			item.setItemType(INTRESTS_LIST[idx]);
			item.setPrice((idx+1)*100);
			return item;
		}
		
		
		private Item[] getCartPurchasedItem(){
			// There can be 0 to 5 items;
			int i = random.nextInt(6);
			
			Item[] items = new Item[i];
			
			for(int j=0;j<i;j++){
				items[j] = getVisitedItem();
			}
			
			return items;		
		}
		
		private User getUser(){
			String tmpString = getDate();
			String phoneNumber= tmpString.replaceAll("-", "");
			phoneNumber = phoneNumber.substring(0, 3)+"-"+phoneNumber.substring(3,6)+"-"+phoneNumber.substring(6,10);
			StringBuffer buffer = new StringBuffer(tmpString);
			
			User user = new User(getName()+tmpString, getName()+tmpString+"@dsacd.com" , phoneNumber, gender, buffer.reverse().toString() , getName()+tmpString);
			
			int idx = random.nextInt(INTRESTS_LIST.length);
			String[] tmpArr = new String[idx];
			for(int i=0;i<idx;i++){
				tmpArr[i] = INTRESTS_LIST[i];
			}
			
			user.setInterests(tmpArr);
			
			int x = random.nextInt(MARITAL_STATUS.length);
			int y = random.nextInt(DEFAULT_MAX_AGE+1);
			
			user.setMaritalStatus(MARITAL_STATUS[x]);
			user.setAge(Integer.toString(y));
			
			return user;
		}
		
		
		public String getDate(){
			return dateFormat.format(new Date());
		}
	}
	
}
