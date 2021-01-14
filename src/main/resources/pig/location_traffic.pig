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
/*
 * Group By IP address and date 
 */
pig_dsacd_user = LOAD 'dsacd_user' USING org.apache.hcatalog.pig.HCatLoader();
pig_dsacd_user_filter = FILTER pig_dsacd_user BY date == '${dsacd_date}';
pig_dsacd_user_group = GROUP pig_dsacd_user_filter BY (ipaddress, date);
pig_dsacd_user_group_count = FOREACH pig_dsacd_user_group GENERATE FLATTEN(group), COUNT(pig_dsacd_user_filter) as traffic_count;
/*
 * load location details
 */
pig_dsacd_location = LOAD 'dsacd_location' USING org.apache.hcatalog.pig.HCatLoader();
/*
 * Join, group and project
 */
pig_dsacd_user_location_join = JOIN pig_dsacd_user_group_count BY group::ipaddress, pig_dsacd_location BY ipaddress USING 'replicated';
pig_dsacd_user_location_join_group = GROUP pig_dsacd_user_location_join BY (pig_dsacd_user_group_count::group::date, 'cf', pig_dsacd_location::location);
pig_dsacd_date_location_traffic = FOREACH pig_dsacd_user_location_join_group GENERATE FLATTEN(group), SUM(pig_dsacd_user_location_join.pig_dsacd_user_group_count::traffic_count);
/*
 * DUMP the results
 */
--DUMP pig_dsacd_date_location_traffic;
rmf ${dsacd_hb_dt_location_ct_tab};
STORE pig_dsacd_date_location_traffic INTO '${dsacd_hb_dt_location_ct_tab}' USING PigStorage(',');
