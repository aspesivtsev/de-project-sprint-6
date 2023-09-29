drop table if exists STV2023081266__DWH.l_user_group_activity cascade;
create table STV2023081266__DWH.l_user_group_activity
(
    hk_l_user_group_activity int not null primary key,
    hk_user_id               int references STV2023081266__DWH.h_users (hk_user_id),
    hk_group_id              int references STV2023081266__DWH.h_groups (hk_group_id),
    load_dt                  timestamp,
    load_src                 varchar(20)
)
    order by load_dt
    segmented by hk_l_user_group_activity all nodes
    partition by load_dt::date group by calendar_hierarchy_day(load_dt::date, 3, 2);
