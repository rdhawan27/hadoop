--
--  Copyright [2015] [Dinesh Sachdev]
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--      http://www.apache.org/licenses/LICENSE-2.0
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
--
REGISTER ${data_fu_jar_path};
register hdfs:///user/root/dsacd/workflows/lib/hive-hcatalog-core.jar;
register hdfs:///user/root/dsacd/workflows/lib/hive-hcatalog-pig-adapter.jar;
--set hbase.zookeeper.quorum '${param_zoo_quorum}';
--set zookeeper.znode.parent '${param_zoo_znode_parent}';

DEFINE Median datafu.pig.stats.Median();

pig_dsacd_user = LOAD 'dsacd_user' USING org.apache.hive.hcatalog.pig.HCatLoader();
pig_dsacd_user_filter = FILTER pig_dsacd_user BY date1 == '${dsacd_date}';
pig_clicks = FOREACH pig_dsacd_user_filter GENERATE sessionid, (long)timestamps, date1;
pig_sessions_group = GROUP pig_clicks BY sessionid;
pig_sessionize = FOREACH pig_sessions_group GENERATE MAX(pig_clicks.date1) as date1, 'cf', group, ( MAX(pig_clicks.timestamps) - MIN(pig_clicks.timestamps)) / 1000.0 / 60 as session_length_min;
--DUMP pig_sessionize;
rmf ${dsacd_hb_dt_sessionid_length_tab};
STORE pig_sessionize INTO '${dsacd_hb_dt_sessionid_length_tab}' USING PigStorage(',');

pig_sessiontimes_all = GROUP pig_sessionize ALL;
pig_sessionize_avg_median = FOREACH pig_sessiontimes_all {
	pig_ordered = ORDER pig_sessionize BY session_length_min;
	GENERATE MAX(pig_ordered.date1) as date1, FLATTEN(Median(pig_ordered.session_length_min)) as med_session, AVG(pig_ordered.session_length_min) as avg_session;
};

--DUMP pig_sessionize_avg_median;
STORE pig_sessionize_avg_median INTO 'dsacd_hb_med_avg_tab' USING
org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:median, cf:average');