insert into STV2023081266__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select hash(hk_user_id, hk_group_id),
       hu.hk_user_id,
       hg.hk_group_id,
       now(),
       's3'
-- saving the fact that htere were actions
from (select distinct user_id, group_id from STV2023081266__STAGING.group_log) l
         left join STV2023081266__DWH.h_users hu on l.user_id = hu.user_id
         left join STV2023081266__DWH.h_groups hg on l.group_id = hg.group_id
-- adding only new data
where hash(hk_user_id, hk_group_id) not in
      (select hk_l_user_group_activity from STV2023081266__DWH.l_user_group_activity);