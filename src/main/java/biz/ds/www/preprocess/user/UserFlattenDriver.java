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
package biz.ds.www.preprocess.user;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import biz.ds.www.exception.CheckParamsException;
import biz.ds.www.preprocess.IPAdressMultiOutFlattenReducer;
import biz.ds.www.preprocess.SessionIDGroupComparator;
import biz.ds.www.preprocess.SessionPartitioner;
import biz.ds.www.preprocess.UserWritableKeyComparator;
import biz.ds.www.types.UserWritable;
import biz.ds.www.utils.CheckParams;

public class UserFlattenDriver extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(UserFlattenDriver.class);
	
	@Override
	public int run(String[] args)  throws Exception{
		try {
			CheckParams.checkParams(args, 3, "<input> <output> <timeDescFlag>", this.getClass().getSimpleName());
		} catch (CheckParamsException e) {
			LOG.error("Error: Checking parameters",e);
			return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(UserFlattenMapper.class);
		job.setPartitionerClass(SessionPartitioner.class);
		
		// By default arrange it in ascending order
		boolean timeDescFlag = false;
		try{
			timeDescFlag = Boolean.parseBoolean(args[2]);
			}
		catch(Exception e ){
			LOG.warn("Can not parse timeDescFlag", e);
		}
		
		if(timeDescFlag){
			job.setSortComparatorClass(UserWritableKeyComparator.class);
		}
		
		job.setGroupingComparatorClass(SessionIDGroupComparator.class);
		job.setReducerClass(IPAdressMultiOutFlattenReducer.class);
		job.setOutputKeyClass(UserWritable.class);
		job.setOutputValueClass(Text.class);
		//So as to not create part-r-00000  file unless there is something to write
		job.setOutputFormatClass(LazyOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UserFlattenDriver(), args);
		System.exit(exitCode);
	}

}
