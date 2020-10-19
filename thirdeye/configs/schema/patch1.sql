create index rootcause_template_metric_idx on rootcause_template_index(metric_id);
create index rootcause_template_config_application_idx ON rootcause_template_index(`application`);
create table if not exists online_detection_data_index (
    base_id bigint(20) not null,
    dataset varchar(200),
    metric varchar(200),
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index online_detection_data_id_idx ON online_detection_data_index(base_id);
create index online_detection_data_dataset_idx ON online_detection_data_index(dataset);
create index online_detection_data_metric_idx ON online_detection_data_index(metric);

create table if not exists anomaly_subscription_group_notification_index (
    base_id bigint(20) not null,
    anomaly_id bigint(20) not null,
    detection_config_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `anomaly_subscription_group_notification_index` ADD UNIQUE `anomaly_subscription_group_notification_index`(anomaly_id);
create index anomaly_subscription_group_anomaly_idx ON anomaly_subscription_group_notification_index(anomaly_id);
create index anomaly_subscription_group_detection_config_idx ON anomaly_subscription_group_notification_index(anomaly_id);
