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
pig_dsacd_user = LOAD 'dsacd_user' USING org.apache.hcatalog.pig.HCatLoader();
pig_dsacd_user_filter = FILTER pig_dsacd_user BY date == '${dsacd_date}';
pig_dsacd_user_group = GROUP pig_dsacd_user_filter BY (device, browser, gender, date);
pig_dsacd_user_group_count = FOREACH pig_dsacd_user_group GENERATE FLATTEN(group), COUNT(pig_dsacd_user_filter) as grp_count;
pig_dsacd_user_group_count_concat = FOREACH pig_dsacd_user_group_count GENERATE CONCAT( CONCAT( CONCAT( CONCAT( group::device, '-'), group::browser), '-'), group::gender), 'cf', group::date, grp_count;
--DUMP pig_dsacd_user_group_count_concat;
rmf ${dsacd_hb_devicebrowgen_dt_ct_tab};
STORE pig_dsacd_user_group_count_concat INTO '${dsacd_hb_devicebrowgen_dt_ct_tab}' USING PigStorage(',');