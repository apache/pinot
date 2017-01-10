
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
create index raw_anomaly_result_start_time_idx on raw_anomaly_result_index(start_time);

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


create table if not exists webapp_config_index (
    name varchar(200) not null,
    collection varchar(200) not null,
    type varchar(100) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `webapp_config_index` ADD UNIQUE `webapp_config_unique_index`(`name`, `collection`, `type`);

create table if not exists dataset_config_index (
    dataset varchar(200) not null,
    active boolean,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_dataset unique(dataset)
) ENGINE=InnoDB;
create index dataset_config_dataset_idx on dataset_config_index(dataset);
create index dataset_config_active_idx on dataset_config_index(active);

create table if not exists metric_config_index (
    name varchar(200) not null,
    dataset varchar(200) not null,
    alias varchar(200) not null,
    active boolean,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `metric_config_index` ADD UNIQUE `unique_metric_index`(`name`, `dataset`);
create index metric_config_name_idx on metric_config_index(name);
create index metric_config_dataset_idx on metric_config_index(dataset);
create index metric_config_alias_idx on metric_config_index(alias);
create index metric_config_active_idx on metric_config_index(active);

create table if not exists dashboard_config_index (
    name varchar(200) not null,
    dataset varchar(200) not null,
    active boolean,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_dashboard_name unique(name)
) ENGINE=InnoDB;
create index dashboard_config_name_idx on dashboard_config_index(name);
create index dashboard_config_dataset_idx on dashboard_config_index(dataset);
create index dashboard_config_active_idx on dashboard_config_index(active);

create table if not exists ingraph_metric_config_index (
    rrd_name varchar(2000) not null,
    metric_name varchar(500) not null,
    dashboard_name varchar(200) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `ingraph_metric_config_index` ADD UNIQUE `ingraph_metric_config_unique_index`(`dashboard_name`, `metric_name`);
create index ingraph_metric_config_metric_name_idx on ingraph_metric_config_index(metric_name);
create index ingraph_metric_config_dashboard_name_idx on ingraph_metric_config_index(dashboard_name);

create table if not exists ingraph_dashboard_config_index (
    name varchar(500) not null,
    bootstrap boolean,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `ingraph_dashboard_config_index` ADD UNIQUE `ingraph_dashboard_config_unique_index`(`name`);
create index ingraph_dashboard_config_name_idx on ingraph_dashboard_config_index(name);
create index ingraph_dashboard_config_bootstrap_idx on ingraph_dashboard_config_index(bootstrap);

create table if not exists override_config_index (
    start_time bigint(20) NOT NULL,
    end_time bigint(20) NOT NULL,
    target_entity varchar(100) NOT NULL,
    active boolean default false,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index override_config_target_entity_idx on override_config_index(target_entity);
create index override_config_target_start_time_idx on override_config_index(start_time);


create table if not exists alert_config_index (
    active boolean,
    name varchar(500) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;

