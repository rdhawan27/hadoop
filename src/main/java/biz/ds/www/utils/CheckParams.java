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

import org.apache.hadoop.util.GenericOptionsParser;
import biz.ds.www.exception.CheckParamsException;

/**
 * Utility class to check parameters usage
 *
 */
public class CheckParams {

	/**
	 * This method checks input array length with provided. If not matched it displays usage prepending className
	 * @param args - input array
	 * @param length - length of array
	 * @param usage - Sequencing of parameters in array
	 * @param className - Name of class to be prepended
	 * @throws CheckParamsException - if usage pattern does not match throw exception
	 */
	public static void checkParams(String [] args, int length, String usage, String className) throws CheckParamsException{
		
		if(args==null || !(args.length==length)){
			printUsage(className, usage);
			throw new CheckParamsException("Define paramerts correctly.");
		}
	}
	
	/**
	 * Print usage pattern
	 * @param className - class name to be prepended before usage string
	 * @param usage - usage literal to be printed
	 */
	public static void printUsage(String className, String usage) {
		System.err.printf("Usage: %s [genericOptions] %s\n\n", className, usage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}
}
