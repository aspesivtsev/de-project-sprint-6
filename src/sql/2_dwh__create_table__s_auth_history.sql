drop table if exists STV2023081266__DWH.s_auth_history;
create table STV2023081266__DWH.s_auth_history
(
    hk_l_user_group_activity int references STV2023081266__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from             int,
    event                    varchar(6)  not null,
    event_ts                 timestamp   not null,
    load_dt                  timestamp   not null,
    load_src                 varchar(20) not null
)
    order by load_dt
    segmented by hk_l_user_group_activity all nodes
    partition by load_dt::date group by calendar_hierarchy_day(load_dt::date, 3, 2);
