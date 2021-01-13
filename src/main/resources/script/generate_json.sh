#!/bin/sh
##
#  Copyright [2015] [Dinesh Sachdev]
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##
####
#Description- This shell script invokes simulator that generates random Json data
#It takes 3 parameters-
# 1 - log4j.properties
# 2 - Number of Threads
# 3 - Sleep time for each Thread
####
CLASSPATH=./lib/dsacd.jar
java -cp $CLASSPATH biz.ds.www.utils.json.WriterJSON ./log4j.properties 8 5