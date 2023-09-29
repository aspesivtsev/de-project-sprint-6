insert into STV2023081266__DWH.s_auth_history (hk_l_user_group_activity, user_id_from, event, event_ts, load_dt,
                                                   load_src)
select hash(hk_user_id, hk_group_id),
       user_id_from,
       event,
       event_ts,
       now(),
       's3'
from STV2023081266__STAGING.group_log l
         left join STV2023081266__DWH.l_user_group_activity hu
                   on hash(l.user_id) = hu.hk_user_id
                       and hash(l.group_id) = hu.hk_group_id
-- Догружаем только инкремент
where event_ts > (select nvl(max(event_ts), '2000-01-01') from STV2023081266__DWH.s_auth_history)
;