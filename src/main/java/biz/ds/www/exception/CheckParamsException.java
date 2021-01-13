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
package biz.ds.www.exception;

public class CheckParamsException extends Exception {

	/**
	 * Create CheckParamsException object with given message
	 * @param msg - message to be set in exception object
	 */
	public CheckParamsException(String msg ) {
		super( msg );
	}
	
	/**
	 * Create CheckParamsException object
	 */
	public CheckParamsException() {
		super();
	}
	
	/**
	 * Create CheckParamsException object with given throwable 
	 * @param e - Throwable object
	 */
	public CheckParamsException(Throwable e ) {
		super(e);
	}
}
