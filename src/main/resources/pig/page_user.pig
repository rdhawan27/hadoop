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
* Group By page and date to identify page hits by unique users and total most page hits in a date
*/
pig_dsacd_user = LOAD 'dsacd_user' USING org.apache.hcatalog.pig.HCatLoader();
pig_dsacd_user_filter = FILTER pig_dsacd_user BY date == '${dsacd_date}';
pig_dsacd_user_group = GROUP pig_dsacd_user_filter BY (page, date);
pig_dsacd_user_page_unique = FOREACH pig_dsacd_user_group {
	pig_unique_emailid = DISTINCT pig_dsacd_user_filter.emailid;
	GENERATE FLATTEN(group) as (page, date), COUNT(pig_unique_emailid) as unique_users, COUNT(pig_dsacd_user_filter) as total_hits;
};
pig_dsacd_user_page_unique_order = ORDER pig_dsacd_user_page_unique BY total_hits desc;
pig_dsacd_user_page_unique_order_fin = FOREACH pig_dsacd_user_page_unique_order GENERATE date, 'cf', page, (total_hits / unique_users) as total_by_uniquser;
--DUMP pig_dsacd_user_page_unique_order_fin;
rmf ${dsacd_hb_dt_page_ct_tab};
STORE pig_dsacd_user_page_unique_order_fin INTO '${dsacd_hb_dt_page_ct_tab}' USING PigStorage(',');