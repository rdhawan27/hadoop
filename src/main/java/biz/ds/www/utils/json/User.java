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

import java.util.Arrays;

public class User {

	private String name;
	private String emailID;
	private String phoneNumber;
	private String gender;
	private String city;
	private String sessionID;
	private String maritalStatus;
	private String age;
	private String[] interests;

	
	public User(String name, String emailID, String phoneNumber, String gender,
			String city, String[] interests) {

		this.name = name;
		this.emailID = emailID;
		this.phoneNumber = phoneNumber;
		this.gender = gender;
		this.city = city;
		this.interests = interests;
	}

	



	public User(String name, String emailID, String phoneNumber, String gender,
			String city) {
	
		this.name = name;
		this.emailID = emailID;
		this.phoneNumber = phoneNumber;
		this.gender = gender;
		this.city = city;
	}


	public User() {
		
	}


	
	public User(String name, String emailID, String phoneNumber, String gender,
			String city, String sessionID) {
		
		this.name = name;
		this.emailID = emailID;
		this.phoneNumber = phoneNumber;
		this.gender = gender;
		this.city = city;
		this.sessionID = sessionID;
	}





	public String getMaritalStatus() {
		return maritalStatus;
	}





	public void setMaritalStatus(String maritalStatus) {
		this.maritalStatus = maritalStatus;
	}





	public String getAge() {
		return age;
	}





	public void setAge(String age) {
		this.age = age;
	}





	public String getSessionID() {
		return sessionID;
	}



	public void setSessionID(String sessionID) {
		this.sessionID = sessionID;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEmailID() {
		return emailID;
	}

	public void setEmailID(String emailID) {
		this.emailID = emailID;
	}

	public String getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String[] getInterests() {
		return interests;
	}

	public void setInterests(String[] interests) {
		this.interests = interests;
	}
	
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		return buffer.append("{")
				.append(name).append("\t")
				.append(emailID).append("\t")
				.append(phoneNumber).append("\t")
				.append(gender).append("\t")
				.append(city).append("\t")
				.append(sessionID).append("\t")
				.append(maritalStatus).append("\t")
				.append(age).append("\t")
				.append(Arrays.toString(interests))
				.append("}").toString();
	}

}
