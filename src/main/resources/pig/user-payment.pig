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
pig_dsacd_user_group = GROUP pig_dsacd_user_filter BY (paymenttype, date);
pig_dsacd_user_group_count = FOREACH pig_dsacd_user_group GENERATE FLATTEN(group), COUNT(pig_dsacd_user_filter) as pay_count;
pig_dsacd_user_group_count_fin = FOREACH pig_dsacd_user_group_count GENERATE group::date, 'cf', group::paymenttype, pay_count;
--DUMP pig_dsacd_user_group_count;
rmf ${dsacd_hb_dt_paytype_ct_tab};
STORE pig_dsacd_user_group_count_fin INTO '${dsacd_hb_dt_paytype_ct_tab}' USING PigStorage(',');