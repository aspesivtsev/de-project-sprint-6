drop table if exists STV2023081266__STAGING.group_log cascade;
create table STV2023081266__STAGING.group_log
(
    group_id     int        not null primary key,
    user_id      int        not null,
    user_id_from int,
    event        varchar(6) not null,
    event_ts     timestamp  not null
)
    order by group_id, event, event_ts
    segmented by hash(group_id) all nodes
    partition by event_ts::date
        group by calendar_hierarchy_day(event_ts::date, 3, 2);
