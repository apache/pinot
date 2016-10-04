-- drop database thirdeye_test; create database thirdeye_test; use thirdeye_test;

create table if not exists generic_json_entity (
    id bigint(20) primary key auto_increment,
    json_val text,
    beanClass varchar(200),
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index generic_json_entity_beanclass_idx on generic_json_entity(beanClass);

create table if not exists anomaly_function_index (
    function_name varchar(200) not null,
    active boolean,
    metric_id bigint(20) not null,
    collection varchar(200),
    metric varchar(200),
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_functionName unique(function_name)
) ENGINE=InnoDB;
create index anomaly_function_name_idx on anomaly_function_index(function_name);

create table if not exists metric_index (
    name varchar(200) not null,
    collection varchar(200) not null,
    derived boolean default false,
    dimension_as_metric boolean default false,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_name unique(name)
) ENGINE=InnoDB;

create table if not exists anomaly_merge_config_index (
    name varchar(200) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_merge_config_name unique(name)
) ENGINE=InnoDB;

create table if not exists email_configuration_index (
    collection varchar(200) not null,
    metric varchar(200),
    active boolean,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;

create table if not exists job_index (
    name varchar(200) not null,
    status varchar(100) not null,
    schedule_start_time bigint(20) not null,
    schedule_end_time bigint(20) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index job_status_idx on job_index(status);

create table if not exists task_index (
    name varchar(200) not null,
    status varchar(100) not null,
    type varchar(100) not null,
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    job_id bigint(20),
    worker_id bigint(20),
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index task_status_idx on task_index(status);
create index task_type_idx on task_index(type);
create index task_job_idx on task_index(job_id);

create table if not exists anomaly_feedback_index (
    type varchar(100) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;

create table if not exists raw_anomaly_result_index (
    function_id bigint(20),
    job_id bigint(20),
    anomaly_feedback_id bigint(20),
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    data_missing boolean default false not null,
    merged boolean default false,
    dimensions varchar(1023),
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index raw_anomaly_result_function_idx on raw_anomaly_result_index(function_id);
create index raw_anomaly_result_feedback_idx on raw_anomaly_result_index(anomaly_feedback_id);
create index raw_anomaly_result_job_idx on raw_anomaly_result_index(job_id);
create index raw_anomaly_result_merged_idx on raw_anomaly_result_index(merged);
create index raw_anomaly_result_data_missing_idx on raw_anomaly_result_index(data_missing);

create table if not exists merged_anomaly_result_index (
    function_id bigint(20),
    anomaly_feedback_id bigint(20),
    metric_id bigint(20),
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    collection varchar(200),
    metric varchar(200),
    dimensions varchar(1023),
    notified boolean default false,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index merged_anomaly_result_function_idx on merged_anomaly_result_index(function_id);
create index merged_anomaly_result_feedback_idx on merged_anomaly_result_index(anomaly_feedback_id);
create index merged_anomaly_result_metric_idx on merged_anomaly_result_index(metric_id);
create index merged_anomaly_result_start_time_idx on merged_anomaly_result_index(start_time);

create table if not exists ingraph_metric_config (
    name varchar(200) not null,
    alias varchar(200) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;

create table if not exists webapp_config_index (
    name varchar(200) not null,
    collection varchar(200) not null,
    type varchar(100) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `webapp_config_index` ADD UNIQUE `unique_index`(`name`, `collection`, `type`);


create table if not exists dataset_config_index (
    dataset varchar(200) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_dataset unique(dataset)
) ENGINE=InnoDB;
create index dataset_config_dataset_idx on dataset_config_index(dataset);

create table if not exists metric_config_index (
    name varchar(200) not null,
    dataset varchar(200) not null,
    alias varchar(200) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `metric_config_index` ADD UNIQUE `unique_metric_index`(`name`, `dataset`);
create index metric_config_name_idx on metric_config_index(name);
create index metric_config_dataset_idx on metric_config_index(dataset);
create index metric_config_alias_idx on metric_config_index(alias);


create table if not exists dashboard_config_index (
    name varchar(200) not null,
    dataset varchar(200) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_dashboard_name unique(name)
) ENGINE=InnoDB;
create index dashboard_config_name_idx on dashboard_config_index(name);
create index dashboard_config_dataset_idx on dashboard_config_index(dataset);

