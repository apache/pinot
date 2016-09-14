-- drop database thirdeye_test; create database thirdeye_test; use thirdeye_test;

create table if not exists generic_json_entity (
    id bigint(20) primary key auto_increment,
    json_val text,
    beanClass varchar(200),
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;

create table if not exists anomaly_function_index (
    function_name varchar(200) not null,
    active boolean,
    metric_id bigint(20) not null,
    collection varchar(200),
    metric varchar(200),
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
     
) ENGINE=InnoDB;


create table if not exists metric_index (
    name varchar(200) not null,
    collection varchar(200) not null,
    derived boolean default false,
    dimension_as_metric boolean default false,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index if not exists anomaly_function_index on metric_index(name);

create table if not exists anomaly_merge_config_index (
    name varchar(200) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
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
create index if not exists job_status_idx on job_index(status);

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
create index if not exists task_status_idx on task_index(status);
create index if not exists task_job_idx on task_index(job_id);

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
create index if not exists raw_anomaly_result_function_idx on raw_anomaly_result_index(function_id);
create index if not exists raw_anomaly_result_feedback_idx on raw_anomaly_result_index(anomaly_feedback_id);
create index if not exists raw_anomaly_result_job_idx on raw_anomaly_result_index(job_id);

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
create index if not exists merged_anomaly_result_function_idx on merged_anomaly_result_index(function_id);
create index if not exists merged_anomaly_result_feedback_idx on merged_anomaly_result_index(anomaly_feedback_id);
create index if not exists merged_anomaly_result_metric_idx on merged_anomaly_result_index(metric_id);

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

