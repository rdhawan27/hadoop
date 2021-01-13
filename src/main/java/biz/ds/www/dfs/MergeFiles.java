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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import biz.ds.www.exception.CheckParamsException;
import biz.ds.www.utils.CheckParams;

/**
 * This class merges the small files from HDFS in to 
 * bigger files in hdfs depicted by file size.
 * <p>args[0] - Name node URI</p>
 * <p>args[1] - Input path of directories where files to be merged are located</p>
 * <p>args[2] - Output path of directory where merged file will be written</p>
 * <p>args[3] - Path of temporary directory </p>
 * <p>args[4] - Size of the merged files</p>
 * 
 * The code reads all the files present int directory and 
 * leave files with '.tmp' extension from reading. Once done files are 
 * merged in 'tmp' folder on hdfs and when respective file size is achieved
 * file is moved to 'processed' folder from 'tmp'
 * 
 */
public class MergeFiles {

	private static final Log LOG = LogFactory.getLog(MergeFiles.class);
	
	private static final long DEFAULT_SIZE = 20971520;
	
	private static String nameNode, inputDir, outputDir, tmpDir;
	
	private static long size;
	
	/*Program execution begins here!!*/
	public static void main(String[] args) throws CheckParamsException, IOException, URISyntaxException  {
		
		LOG.info("Starting MergeFiles:main[args]...");
		
		CheckParams.checkParams(args, 5, "<NameNodeURI> <InputDirPath> <OutputDirPath> <TempDirPath> <SizeInBytes>", MergeFiles.class.getSimpleName());
		
		nameNode = args[0];
		inputDir = args[1];
		outputDir = args[2];
		tmpDir = args[3];
		
		try{
			size = Long.parseLong(args[4]);
		}catch(NumberFormatException e){
			LOG.error("Setting Default value of size",e);
			size = DEFAULT_SIZE;
		}
		
		Configuration configuration = new Configuration();

		FileSystem hdfs = FileSystem.get(new URI(nameNode), configuration);
		
		String outTmpFilePath = tmpDir+"/mergefile.tmp";
		
		//Get tmp file output stream
		FSDataOutputStream out = getOutStream(outTmpFilePath, hdfs);
		
		//Copy files from input directory to tmp file
		copyFile(hdfs, inputDir, out);
		
		Path tmpoutPath = new Path(outTmpFilePath);
		
		long length = hdfs.getLength(tmpoutPath);
		
		if(LOG.isDebugEnabled())
			LOG.debug("Length of file is: "+ length);
		
		//Check length of tmp file if it greater than size move it to processed dir
		if(length>= size){
			String outProcessedFile = outputDir+"/"+System.currentTimeMillis();
			
			LOG.info("Moving file: "+outTmpFilePath+"\n to: "+outProcessedFile);
			
			hdfs.rename(tmpoutPath, new Path(outProcessedFile));			
		}		
		
	}
	
	/**
	 * Get file system data output stream
	 * @param outputPath - output file path
	 * @param hdfs - distributed file system
	 * @return file system data output stream
	 * @throws IOException - If processing fails
	 */
	private static FSDataOutputStream getOutStream(String outputPath, FileSystem hdfs) throws IOException{
		
		Progressable progressable = new Progressable()  {
		    public void progress() {
		    System.out.print("*");
		    }
	    };
	    
	    Path outPath = new Path(outputPath);
	    
	    FSDataOutputStream out =null;
	    
	    if(!hdfs.exists(outPath))
	    {
	    	//short s = 1;
	    	if(LOG.isDebugEnabled())
	    		LOG.debug("File does not exist so creating one...");
	    	//out =hdfs.create(outPath,s, progressable);
	    	out =hdfs.create(outPath, progressable);
	    }
	    else{
	    	if(LOG.isDebugEnabled())
	    		LOG.debug("File already exist. So appending same...");
	    	out = hdfs.append(outPath,4096,progressable);	    	
	    }
	    
	   return out;
	}
	
	/**
	 * Copy files in input directory to output stream
	 * @param hdfs - distributed file system
	 * @param inputDir - path of input directory
	 * @param out - file system data output stream
	 * @throws FileNotFoundException - If input directory is not found
	 * @throws IllegalArgumentException - If invalid input directory is provided
	 * @throws IOException - If processing fails 
	 */
	private static void copyFile(FileSystem hdfs, String inputDir, FSDataOutputStream out) throws FileNotFoundException, IllegalArgumentException, IOException{
		
		FileStatus[] fileStatus = hdfs.listStatus(new Path(inputDir), new RegxExcludeTempFiles("([^\\s]+(\\.(?i)(tmp))$)"));
		    
		Path[] paths = FileUtil.stat2Paths(fileStatus);		    
	   
		LOG.info("Copying ...");
		
		for(Path path : paths)
	    {
	    	if(!hdfs.isFile(path)){
	    		LOG.info(path.toString()+" is not a file. So skipping it...");
	    	}
	    	else{
	    		
	    		if(LOG.isDebugEnabled());
	    			LOG.debug("Copying "+path.toString()+"...");
	    			
	    		InputStream inputStream =null;
	    		
	    		try{
	    			inputStream  = hdfs.open(path);
	    			
	    			// do not close output stream until all files are copied
	    			IOUtils.copyBytes(inputStream, out, 4096, false);
	    		}finally{
	    			IOUtils.closeStream(inputStream);
	    		}
	    		
	    		if(LOG.isDebugEnabled());
    				LOG.debug("Deleting "+path.toString()+"...");
	    		hdfs.delete(path);
	    	}
	    }
		
		LOG.info("Closing output stream.");
		IOUtils.closeStream(out);
	}
}