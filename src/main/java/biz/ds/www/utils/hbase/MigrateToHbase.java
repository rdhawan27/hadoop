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
package biz.ds.www.utils.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
/*import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;*/
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import biz.ds.www.dfs.MoveFiles;
import biz.ds.www.exception.CheckParamsException;
import biz.ds.www.utils.CheckParams;

public class MigrateToHbase {

	private static final Log LOG = LogFactory.getLog(MigrateToHbase.class);
	//private static final String SPLIT_CHARACTER = "";
	private static final String SPLIT_CHARACTER = ",";
	
	private static String nameNode, inputDir, hbTN;
	
	/**
	 * @param args
	 * @throws CheckParamsException 
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws CheckParamsException, IOException, URISyntaxException {
		LOG.info("Starting MigrateToHbase:main[args]...");
		
		CheckParams.checkParams(args, 3, "<NameNodeURI> <InputDirPath> <HBaseTableName>", MigrateToHbase.class.getSimpleName());
		
		List<Path> paths;
		Set<String> columnFamily;
		Map<String, Put> putMap ;
		
		nameNode = args[0];
		inputDir = args[1];
		hbTN = args[2];
		
		paths = new ArrayList<Path>();
		columnFamily = new TreeSet<String>();
		putMap = new TreeMap<String, Put>();
		
		Configuration configuration = new Configuration();

		FileSystem hdfs = FileSystem.get(new URI(nameNode), configuration);
		
		Path inPath =  new Path(inputDir);
		
		//Get the list of files
		MoveFiles.getList(paths, hdfs, inPath);
		
		for(Path path : paths){
			BufferedReader br =null;
			
			try{
				br=new BufferedReader(new InputStreamReader(hdfs.open(path)));
				String line;
                line=br.readLine();
                
                while (line != null){
                		String[] splits = line.split(SPLIT_CHARACTER);
                		
                		if(LOG.isDebugEnabled())
                			LOG.debug("Read line splitted: "+Arrays.toString(line.split(SPLIT_CHARACTER)));
                		
                		Put p = putMap.get(splits[0]);
                		
                		if(p == null){
                			p = new Put(Bytes.toBytes(splits[0]));
                		}
                		
                		//pattern - rk, cf, cl, vl
                		p.addColumn(Bytes.toBytes(splits[1]), Bytes.toBytes(splits[2]), Bytes.toBytes(splits[3]));
                		
                		putMap.put(splits[0], p);
                		columnFamily.add(splits[1]);
                		
                        line=br.readLine();
                }
			}finally{
				try{
					br.close();
				}catch(Exception e){
					LOG.error("BufferedReader close failure",e);
				}
			}
			
		}
		
		Configuration conf = HBUtils.getConf();
		ClusterConnection conn = HBUtils.getHConnection(conf);
		Admin admin = HBUtils.getHBaseAdmin(conn);
		
		for(String cf : columnFamily){
			HBUtils.createTableCF(admin, hbTN, cf );
		}
		
		/*HTableDescriptor tableInterface = HBUtils.getHTableInterface( hbTN);*/
		
		Table table = conn.getTable(TableName.valueOf(hbTN.getBytes()));
		for(Map.Entry<String, Put> entry: putMap.entrySet()){
			table.put(entry.getValue());
		}
		
		HBUtils.close(admin, table, conn);
		
	}

}
