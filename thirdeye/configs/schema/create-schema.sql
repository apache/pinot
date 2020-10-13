-- create alias if not exists TO_UNIXTIME as $$ long unix_timestamp(java.sql.Timestamp timestamp) { return
-- (long) (timestamp.getTime() / 1000L); } $$;

create table if not exists generic_json_entity (
    id bigint(20) primary key auto_increment,
    json_val text,
    beanClass varchar(200),
    create_time timestamp default current_timestamp,
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
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_functionName unique(function_name)
) ENGINE=InnoDB;
create index anomaly_function_name_idx on anomaly_function_index(function_name);
create index anomaly_function_base_id_idx ON anomaly_function_index(base_id);

create table if not exists job_index (
    name varchar(200) not null,
    status varchar(100) not null,
    type varchar(100) not null,
    config_id bigint(20),
    schedule_start_time bigint(20) not null,
    schedule_end_time bigint(20) not null,
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index job_status_idx on job_index(status);
create index job_type_idx on job_index(type);
create index job_config_id_idx on job_index(config_id);
create index job_base_id_idx ON job_index(base_id);

create table if not exists task_index (
    name varchar(200) not null,
    status varchar(100) not null,
    type varchar(100) not null,
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    job_id bigint(20),
    worker_id bigint(20),
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index task_status_idx on task_index(status);
create index task_type_idx on task_index(type);
create index task_job_idx on task_index(job_id);
create index task_base_id_idx ON task_index(base_id);
create index task_name_idx ON task_index(name);
create index task_start_time_idx on task_index(start_time);
create index task_create_time_idx on task_index(create_time);
create index task_status_start_time_idx on task_index(status, start_time);

create table if not exists anomaly_feedback_index (
    type varchar(100) not null,
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index anomaly_feedback_base_id_idx ON anomaly_feedback_index(base_id);

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
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index raw_anomaly_result_function_idx on raw_anomaly_result_index(function_id);
create index raw_anomaly_result_feedback_idx on raw_anomaly_result_index(anomaly_feedback_id);
create index raw_anomaly_result_job_idx on raw_anomaly_result_index(job_id);
create index raw_anomaly_result_merged_idx on raw_anomaly_result_index(merged);
create index raw_anomaly_result_data_missing_idx on raw_anomaly_result_index(data_missing);
create index raw_anomaly_result_start_time_idx on raw_anomaly_result_index(start_time);
create index raw_anomaly_result_base_id_idx on raw_anomaly_result_index(base_id);

create table if not exists merged_anomaly_result_index (
    function_id bigint(20),
    detection_config_id bigint(20),
    anomaly_feedback_id bigint(20),
    metric_id bigint(20),
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    collection varchar(200),
    metric varchar(200),
    dimensions varchar(1023),
    notified boolean default false,
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    child boolean,
    version int(10)
) ENGINE=InnoDB;
create index merged_anomaly_result_function_idx on merged_anomaly_result_index(function_id);
create index merged_anomaly_result_feedback_idx on merged_anomaly_result_index(anomaly_feedback_id);
create index merged_anomaly_result_metric_idx on merged_anomaly_result_index(metric_id);
create index merged_anomaly_result_start_time_idx on merged_anomaly_result_index(start_time);
create index merged_anomaly_result_base_id_idx on merged_anomaly_result_index(base_id);
create index merged_anomaly_result_detection_config_id_idx on merged_anomaly_result_index(detection_config_id);

create table if not exists dataset_config_index (
    dataset varchar(200) not null,
    display_name varchar(200),
    active boolean,
    requires_completeness_check boolean,
    last_refresh_time bigint(20) default 0,
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_dataset unique(dataset)
) ENGINE=InnoDB;
create index dataset_config_dataset_idx on dataset_config_index(dataset);
create index dataset_config_active_idx on dataset_config_index(active);
create index dataset_config_requires_completeness_check_idx on dataset_config_index(requires_completeness_check);
create index dataset_config_base_id_idx ON dataset_config_index(base_id);
create index dataset_config_display_name_idx on dataset_config_index(display_name);
create index dataset_config_last_refresh_time_idx on dataset_config_index(last_refresh_time);

create table if not exists metric_config_index (
    name varchar(200) not null,
    dataset varchar(200) not null,
    alias varchar(400) not null,
    active boolean,
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `metric_config_index` ADD UNIQUE `unique_metric_index`(`name`, `dataset`);
create index metric_config_name_idx on metric_config_index(name);
create index metric_config_dataset_idx on metric_config_index(dataset);
create index metric_config_alias_idx on metric_config_index(alias);
create index metric_config_active_idx on metric_config_index(active);
create index metric_config_base_id_idx ON metric_config_index(base_id);

create table if not exists override_config_index (
    start_time bigint(20) NOT NULL,
    end_time bigint(20) NOT NULL,
    target_entity varchar(100) NOT NULL,
    active boolean default false,
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index override_config_target_entity_idx on override_config_index(target_entity);
create index override_config_target_start_time_idx on override_config_index(start_time);
create index override_config_base_id_idx ON override_config_index(base_id);

create table if not exists alert_config_index (
    active boolean,
    name varchar(256) not null,
    application VARCHAR(256) not null,
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_alert_name UNIQUE (name)
) ENGINE=InnoDB;
create index alert_config_application_idx on alert_config_index(application);
create index alert_config_name_idx on alert_config_index(name);
create index alert_config_base_id_idx ON alert_config_index(base_id);

create table if not exists data_completeness_config_index (
    dataset varchar(200) not null,
    date_to_check_in_ms bigint(20) not null,
    date_to_check_in_sdf varchar(20),
    data_complete boolean,
    percent_complete double,
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `data_completeness_config_index` ADD UNIQUE `data_completeness_config_unique_index`(`dataset`, `date_to_check_in_ms`);
create index data_completeness_config_dataset_idx on data_completeness_config_index(dataset);
create index data_completeness_config_date_idx on data_completeness_config_index(date_to_check_in_ms);
create index data_completeness_config_sdf_idx on data_completeness_config_index(date_to_check_in_sdf);
create index data_completeness_config_complete_idx on data_completeness_config_index(data_complete);
create index data_completeness_config_percent_idx on data_completeness_config_index(percent_complete);
create index data_completeness_config_base_id_idx ON data_completeness_config_index(base_id);

create table if not exists event_index (
  name VARCHAR (100),
  event_type VARCHAR (100),
  start_time bigint(20) not null,
  end_time bigint(20) not null,
  metric_name VARCHAR(200),
  service_name VARCHAR (200),
  base_id bigint(20) not null,
  create_time timestamp default current_timestamp,
  update_time timestamp default current_timestamp,
  version int(10)
) ENGINE=InnoDB;
create index event_event_type_idx on event_index(event_type);
create index event_start_time_idx on event_index(start_time);
create index event_end_time_idx on event_index(end_time);
create index event_base_id_idx ON event_index(base_id);

create table if not exists detection_status_index (
  function_id bigint(20),
  dataset varchar(200) not null,
  date_to_check_in_ms bigint(20) not null,
  date_to_check_in_sdf varchar(20),
  detection_run boolean,
  base_id bigint(20) not null,
  create_time timestamp default current_timestamp,
  update_time timestamp default current_timestamp,
  version int(10)
) ENGINE=InnoDB;
ALTER TABLE `detection_status_index` ADD UNIQUE `detection_status_unique_index`(`function_id`, `date_to_check_in_sdf`);
create index detection_status_dataset_idx on detection_status_index(dataset);
create index detection_status_date_idx on detection_status_index(date_to_check_in_ms);
create index detection_status_sdf_idx on detection_status_index(date_to_check_in_sdf);
create index detection_status_run_idx on detection_status_index(detection_run);
create index detection_status_function_idx on detection_status_index(function_id);
create index detection_status_base_id_idx ON detection_status_index(base_id);

create table if not exists autotune_config_index (
    function_id bigint(20),
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    performance_evaluation_method varchar(200),
    autotune_method varchar(200),
    base_id bigint(20) not null,
   create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index autotune_config_function_idx on autotune_config_index(function_id);
create index autotune_config_autoTuneMethod_idx on autotune_config_index(autotune_method);
create index autotune_config_performanceEval_idx on autotune_config_index(performance_evaluation_method);
create index autotune_config_start_time_idx on autotune_config_index(start_time);
create index autotune_config_base_id_idx ON autotune_config_index(base_id);

create table if not exists classification_config_index (
    name varchar(200) not null,
    active boolean,
    base_id bigint(20) not null,
   create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `classification_config_index` ADD UNIQUE `classification_config_unique_index`(`name`);
create index classification_config_name_index on classification_config_index(name);
create index classification_config_base_id_idx ON classification_config_index(base_id);

create table if not exists entity_to_entity_mapping_index (
    from_urn varchar(256) not null,
    to_urn varchar(256) not null,
    mapping_type varchar(500) not null,
    base_id bigint(20) not null,
   create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `entity_to_entity_mapping_index` ADD UNIQUE `entity_mapping_unique_index`(`from_urn`, `to_urn`);
create index entity_mapping_from_urn_idx on entity_to_entity_mapping_index(from_urn);
create index entity_mapping_to_urn_idx on entity_to_entity_mapping_index(to_urn);
create index entity_mapping_type_idx on entity_to_entity_mapping_index(mapping_type);
create index entity_to_entity_mapping_base_id_idx ON entity_to_entity_mapping_index(base_id);

create table if not exists grouped_anomaly_results_index (
    alert_config_id bigint(20) not null,
    dimensions varchar(1023),
    end_time bigint(20) not null,
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index grouped_anomaly_results_alert_config_id on grouped_anomaly_results_index(alert_config_id);
create index grouped_anomaly_results_base_id_idx ON grouped_anomaly_results_index(base_id);

create table if not exists onboard_dataset_metric_index (
  dataset_name varchar(200) not null,
  metric_name varchar(256),
  data_source varchar(500) not null,
  onboarded boolean,
  base_id bigint(20) not null,
  create_time timestamp default current_timestamp,
  update_time timestamp default current_timestamp,
  version int(10)
) ENGINE=InnoDB;
create index onboard_dataset_idx on onboard_dataset_metric_index(dataset_name);
create index onboard_metric_idx on onboard_dataset_metric_index(metric_name);
create index onboard_datasource_idx on onboard_dataset_metric_index(data_source);
create index onboard_onboarded_idx on onboard_dataset_metric_index(onboarded);
create index onboard_dataset_metric_base_id_idx ON onboard_dataset_metric_index(base_id);

create table if not exists config_index (
    namespace varchar(64) not null,
    name varchar(128) not null,
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `config_index` ADD UNIQUE `config_unique_index`(`namespace`, `name`);
create index config_namespace_idx on config_index(namespace);
create index config_name_idx on config_index(name);
create index config_base_id_idx ON config_index(base_id);

create table application_index (
  base_id bigint(20) not null,
  create_time timestamp default current_timestamp,
  update_time timestamp default current_timestamp,
  application VARCHAR (200) not null,
  recipients VARCHAR(1000) NOT NULL
);
create index application_application_idx on application_index(application);

create table if not exists alert_snapshot_index (
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index alert_snapshot_base_id_idx ON alert_snapshot_index(base_id);

create table if not exists rootcause_session_index (
    base_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    name varchar(256),
    owner varchar(32),
    previousId bigint(20),
    anomalyId bigint(20),
    anomaly_range_start bigint(8),
    anomaly_range_end bigint(8),
    created bigint(8),
    updated bigint(8),
    version int(10)
) ENGINE=InnoDB;
create index rootcause_session_name_idx on rootcause_session_index(name);
create index rootcause_session_owner_idx on rootcause_session_index(owner);
create index rootcause_session_previousId_idx on rootcause_session_index(previousId);
create index rootcause_session_anomalyId_idx on rootcause_session_index(anomalyId);
create index rootcause_session_anomaly_range_start_idx on rootcause_session_index(anomaly_range_start);
create index rootcause_session_anomaly_range_end_idx on rootcause_session_index(anomaly_range_end);
create index rootcause_session_created_idx on rootcause_session_index(created);
create index rootcause_session_updated_idx on rootcause_session_index(updated);
create index rootcause_session_base_id_idx ON rootcause_session_index(base_id);

create table if not exists session_index (
    base_id bigint(20) not null,
    session_key CHAR(64) not null,
    principal_type VARCHAR(32),
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `session_index` ADD UNIQUE `session_unique_index`(session_key);
create index session_base_id_idx ON session_index(base_id);
create index session_key_idx ON session_index(session_key);
create index session_principal_type_idx ON session_index(principal_type);

create table if not exists detection_config_index (
    base_id bigint(20) not null,
    `name` VARCHAR(256) not null,
    active BOOLEAN,
    created_by VARCHAR(256),
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `detection_config_index` ADD UNIQUE `detection_config_unique_index`(`name`);
create index detection_config_base_id_idx ON detection_config_index(base_id);
create index detection_config_name_idx ON detection_config_index(`name`);
create index detection_config_active_idx ON detection_config_index(active);
create index detection_config_created_by_index ON detection_config_index(created_by);

create table if not exists detection_alert_config_index (
    base_id bigint(20) not null,
    application VARCHAR(128),
    `name` VARCHAR(256) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `detection_alert_config_index` ADD UNIQUE `detection_alert_config_unique_index`(`name`);
create index detection_alert_config_base_id_idx ON detection_alert_config_index(base_id);
create index detection_alert_config_name_idx ON detection_alert_config_index(`name`);
create index detection_alert_config_application_idx ON detection_alert_config_index(`application`);

create table if not exists evaluation_index (
    base_id bigint(20) not null,
    detection_config_id bigint(20) not null,
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    detectorName VARCHAR(128),
    mape double,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `evaluation_index` ADD UNIQUE `evaluation_index`(`detection_config_id`, `start_time`, `end_time`);
create index evaluation_base_id_idx ON evaluation_index(base_id);
create index evaluation_detection_config_id_idx ON evaluation_index(detection_config_id);
create index evaluation_detection_start_time_idx on evaluation_index(start_time);

create table if not exists rootcause_template_index (
    base_id bigint(20) not null,
    `name` VARCHAR(256) not null,
    application VARCHAR(128),
    owner varchar(32) not null,
    metric_id bigint(20) not null,
    create_time timestamp default current_timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `rootcause_template_index` ADD UNIQUE `rootcause_template_index`(`name`);
create index rootcause_template_id_idx ON rootcause_template_index(base_id);
create index rootcause_template_owner_idx ON rootcause_template_index(owner);
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
