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
package biz.ds.www.utils.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class HiveJdbcClient {

	private static String strDriverName = "org.apache.hive.jdbc.HiveDriver";
	
	private static final Log LOG = LogFactory.getLog(HiveJdbcClient.class);
	
	private static final String ALTER_TABLE_ADD_PARTITION_SQL =
			"alter table $TABLE_NAME  add if not exists partition ($PARTITION_CRITERIA)";
	
	private static final String TABLE_NAME_PROP = "$TABLE_NAME";
	
	private static final String PARTITION_CRITERIA_PROP = "$PARTITION_CRITERIA";
	
	public static void createPartition(Connection connection, String tableName, String partitionCriteria) throws SQLException{
		LOG.info("Going to create partition...");
		Statement st = null;
		try{
			st = createStatement(connection);
			
			String sql = ALTER_TABLE_ADD_PARTITION_SQL.replace(TABLE_NAME_PROP, tableName).
					replace(PARTITION_CRITERIA_PROP, partitionCriteria);
			//System.out.println(sql);
			boolean val = st.execute(sql);
			
			if(LOG.isDebugEnabled())
				LOG.debug("Executed sql: "+sql+" ,Result set returned: "+val);
		}finally{
			try{
				st.close();
			}catch(Exception e ){
				LOG.error("Unable to close statement.\n"+e.getMessage());
			}
		}
		
	}
	
	public static Connection getConnection(String url, String user, String pass) throws ClassNotFoundException, SQLException{
		try {
			Class.forName(strDriverName);
		} catch (ClassNotFoundException e) {
			LOG.error("Partitioning failed.",e);
			throw e;
		}
		
		return DriverManager.getConnection(	url, user, pass);
	}
	
	public static Statement createStatement(Connection connection) throws SQLException{
		return connection.createStatement();
	}
	
	public static void closeConnection(Connection connection){
		try{
			connection.close();
		}catch(Exception e){
			LOG.error("Failed to close connection",e);
		}
	}
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		
		Connection con = getConnection("jdbc:hive2://192.168.190.128:10000","","");
		createPartition(con, "dsacd_user", "ipaddress='172.168.56.23', date='2015-10-08'");
		closeConnection(con);
		
		/*
		try {
			Class.forName(strDriverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("No class found");
			System.exit(1);
		}
		
		Connection con = DriverManager.getConnection(	"jdbc:hive2://192.168.190.128:10000","","");
        Statement stmt = con.createStatement();
        String strTableName = "dsacd_user";

        String sql = " alter table dsacd_user  add if not exists partition ()";
        
        System.out.println(stmt.execute(sql));*/

       /* while (res1.next()){
            System.out.println(res1.getString(1) + "\t" + res1.getString(2));
        }*/
		 //jdbc:hive2://localhost:10000
	}

}
