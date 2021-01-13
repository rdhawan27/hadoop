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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import biz.ds.www.exception.CheckParamsException;
import biz.ds.www.utils.CheckParams;
import biz.ds.www.utils.hive.HiveJdbcClient;

/**
 * This class is used to recursively calculate list
 * of files in given input directory and then move the file 
 * to provided output directory. When files are moved time stamp is 
 * prepended to file name so as to have unique file name each time 
 * this class is executed.
 *<p>args[0] - Name node</p>
 *<p>args[1] - Input directory</p>
 *<p>args[2] - Output directory</p>
 *<p>args[3] - Flag to indicate to remove input directory after move</p>
 *<p>args[4] - Flag to indicate if hive partition needs to be created</p>
 *<p>args[5] - Hive URL</p>
 *<p>args[6] - Hive User</p>
 *<p>args[7] - Hive Password</p>
 */
public class MoveFiles {

	private static final Log LOG = LogFactory.getLog(MoveFiles.class);
	
		
	/**
	 * @param args
	 * @throws CheckParamsException 
	 * @throws URISyntaxException 
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws CheckParamsException, IOException, URISyntaxException, ClassNotFoundException, SQLException {
		
		LOG.info("Starting MoveFiles:main[args]...");
		
		CheckParams.checkParams(args, 8, "<NameNode> <InputDirPath> <OutputDirPath> <RemoveInputDirAfterMove> <CreateHivePartition> <HiveURL> <HiveUser> <HivePass>", MoveFiles.class.getSimpleName());
		
		String inputDir, outputDir, nameNode, hiveURL, hiveUser, hivePass;
		List<Path> paths;
		
		boolean removeInputDir=false, createHiveParttion = true;
		
		paths = new ArrayList<Path>();
		nameNode = args[0];
		inputDir = args[1];
		outputDir = args[2];
		
		try{
			removeInputDir = Boolean.parseBoolean(args[3]);
			createHiveParttion = Boolean.parseBoolean(args[4]);
		}
		catch(Exception e){
			LOG.warn("Got exception in parsing RemoveInputDirAfterMove/CreateHivePartition flag.");
		}
		
		hiveURL = args[5];
		hiveUser = args[6];
		hivePass = args[7];
		
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI(nameNode), configuration);
		Path inPath =  new Path(inputDir);
		
		//cat the list of files recursively
		getList(paths, hdfs, inPath);
		
		// move files
		moveFiles(paths, hdfs, inputDir, outputDir);
		
		//hive partition creation
		if(createHiveParttion){
			LOG.info("create hive partition is set to "+createHiveParttion);
			createPartition(paths, inputDir, outputDir, hiveURL, hiveUser, hivePass);
		}
		
		//removing input directory
		if(removeInputDir){
			LOG.info("Removing input directory: "+inputDir);
			hdfs.delete(inPath , true);
		}
		
	}
	
	public static void createPartition(List<Path> paths, String inputDir, String outputDir, String hiveURL, String hiveUser, String hivePass) throws ClassNotFoundException, SQLException{
		String tableName = outputDir.substring(outputDir.lastIndexOf("/")+1).trim();
		
		if(LOG.isDebugEnabled())
			LOG.debug(String.format("Getting connection with properties: HIVE URL: %s, USER: %s, PASSWORD: %s", hiveURL, hiveUser, hivePass));
		
		Connection conn = HiveJdbcClient.getConnection(hiveURL, hiveUser, hivePass);
		try{
			for(Path path: paths){
				//removing hdfs://.../ upto last '/'
				String partitionCrtiteria = path.toString().replace(inputDir+"/", "");
				// remove /filename
				partitionCrtiteria = partitionCrtiteria.substring(0,partitionCrtiteria.lastIndexOf("/"));
				// replace to add single quotes
				partitionCrtiteria = partitionCrtiteria.replaceAll("=", "='").replaceAll("/", "', ");
				// append last single quote
				partitionCrtiteria = partitionCrtiteria+"'";
				
				if(LOG.isDebugEnabled())
					LOG.debug(String.format("Table name: %s, Partition criteria: %s",tableName, partitionCrtiteria));
				//System.out.println(String.format("Table name: %s, Partition criteria: %s",tableName, partitionCrtiteria));
				HiveJdbcClient.createPartition(conn, tableName, partitionCrtiteria);
			}
		}finally{
			HiveJdbcClient.closeConnection(conn);
		}		
	}
	
	public static void moveFiles(List<Path> listPaths, FileSystem hdfs, String inputDir, String outputDir) throws IllegalArgumentException, IOException{
		
		long timestamp = System.currentTimeMillis();
		
		LOG.info("Moving file at timestamp:"+timestamp);
		
		for(Path path : listPaths){
			
			String currPathName = path.toString(); // Use toString instead of getName() to get absolute path
			
			//replacing inputDir to outputDir and appending timestamp to file name
			StringBuffer newPathName = new StringBuffer(currPathName.replace(inputDir, outputDir));
			newPathName.append("-").append(timestamp);
			
			String nwP = newPathName.toString();
			
			String dir = nwP.substring(0,nwP.lastIndexOf("/"));
			
			
			if(LOG.isDebugEnabled()){
				LOG.debug("Creating directory: "+dir);
				LOG.debug("Renaming current path: "+currPathName+"\n to new path: "+nwP);
			}
			
			hdfs.mkdirs(new Path(dir));
			hdfs.rename(path, new Path(nwP));
			
		}
	}
	
	public static void getList(List<Path> listPaths, FileSystem hdfs, Path inputDir) throws IOException{
		FileStatus[] fileStatus = hdfs.listStatus(inputDir);
	    
		Path[] paths = FileUtil.stat2Paths(fileStatus);	
		
		LOG.info("Getting list of files from given input directory ...");
		
		for(Path path : paths)
	    {
			//to ignore _SUCCESS file in move
			if(hdfs.isFile(path)){
				if(!(path.getName().startsWith("_"))){
					if(LOG.isDebugEnabled())
						LOG.debug(path.toString()+" is a file. Adding this to list...");
					listPaths.add(path);
				}else{
					LOG.warn(path.toString()+" is a file but starts with '_'. So, skipping it.");
				}
	    	}else{
	    		//if path is a directory
	    		if(LOG.isDebugEnabled())
					LOG.debug(path.toString()+" is a directory. Recursilvely calling getList() to fetch files in this directory.");
	    		getList(listPaths, hdfs,path);
	    	}
	    }
	}

}
