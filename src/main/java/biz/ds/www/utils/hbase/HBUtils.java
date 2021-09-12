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
  *//*

package biz.ds.www.utils.hbase;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.HConnection;
//import org.apache.hadoop.hbase.client.HConnectionManager;
//import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;

public class HBUtils {

	private static final Log LOG = LogFactory.getLog(HBUtils.class);
	
	//HTableDescriptor desc = new HTableDescriptor("twits");
	
	//Configuration conf = HBaseConfiguration.create();
	
	*/
/*public static HTableDescriptor getTD(){
		
	}*//*

	
	public  static Configuration getConf() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "sandbox-hdp.hortonworks.com");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		// conf.set("hbase.master","192.168.190.128:60000");
		// conf.set("hbase.zookeeper.property.clientPort","2181");
		// conf.set("hbase.rootdir","hdfs://sandbox.hortonworks.com:8020/apps/hbase/data");
		return conf;
	}

	public static ClusterConnection getHConnection (Configuration conf) throws IOException{
		//return HConnectionManager.createConnection(conf);
		return (ClusterConnection)  ConnectionFactory.createConnection(conf);
	}
	
	public static HTableDescriptor getHTableInterface( String tableName) throws IOException{
		//return hC.getTable(tableName);
		return  new HTableDescriptor(TableName.valueOf(tableName.getBytes()));
	}
	
	public static Admin getHBaseAdmin(ClusterConnection con) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		return con.getAdmin();
		//return new HBaseAdmin(con);
	}
	
	public static void createTableCF(Admin admin, String tableName, String columnFamily) throws IOException{
		
		boolean isTPre = admin.isTableAvailable(TableName.valueOf(tableName.getBytes()));
		
		if(LOG.isDebugEnabled())
			LOG.debug("Is table persent in HBase: "+isTPre);
		
		HTableDescriptor table = null;
		HColumnDescriptor  col = null;
		
		if(!isTPre){
			table = new HTableDescriptor(TableName.valueOf(tableName.getBytes()));
			col = new HColumnDescriptor(columnFamily);
			table.addFamily(col);
			admin.createTable(table);
			LOG.info("Created table: "+table);
		}
		else{
			table = admin.getTableDescriptor(TableName.valueOf(tableName.getBytes()));
			col = table.getFamily(columnFamily.getBytes());
			if(col == null){
				col = new HColumnDescriptor(columnFamily);
				
				LOG.info("Adding column family: "+columnFamily);
				admin.disableTable(TableName.valueOf(tableName.getBytes()));
				admin.addColumn(TableName.valueOf(tableName.getBytes()), col);
				admin.enableTable(TableName.valueOf(tableName.getBytes()));
			}
		}	
		
	}
	
	public static void close(Admin admin, Table tableInterface, ClusterConnection hConnection){
		LOG.info("Closing admin, tableInterface, hConnection...");
		
		try{
			admin.close();
		}catch(Exception e){
			LOG.error("Error in closing HBaseAdmin",e);
		}
		
		try{
			tableInterface.close();
		}catch(Exception e){
			LOG.error("Error in closing HTableInterface",e);
		}
		
		try{
			hConnection.close();
		}catch(Exception e){
			LOG.error("Error in closing HConnection",e);
		}
	}

	public static void main(String[] args) {
		try {
			createTableCF(getHBaseAdmin(getHConnection(getConf())),"twits12", "twits112");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
*/
