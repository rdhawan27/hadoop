#!/bin/sh
####
#Description- This shell script invokes simulator that generates random Json data
#It takes 3 parameters-
# 1 - log4j.properties
# 2 - Number of Threads
# 3 - Sleep time for each Thread
####
CLASSPATH=./lib/dsacd.jar
java -cp $CLASSPATH biz.ds.www.utils.json.WriterJSON ./log4j.properties 8 5