
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
    type varchar(100) not null,
    config_id bigint(20),
    schedule_start_time bigint(20) not null,
    schedule_end_time bigint(20) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index job_status_idx on job_index(status);
create index job_type_idx on job_index(type);
create index job_config_id_idx on job_index(config_id);

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
create index raw_anomaly_result_base_id_idx on raw_anomaly_result_index(base_id);

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
create index merged_anomaly_result_base_id_idx on merged_anomaly_result_index(base_id);

create table if not exists dataset_config_index (
    dataset varchar(200) not null,
    active boolean,
    requires_completeness_check boolean,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10),
    CONSTRAINT uc_dataset unique(dataset)
) ENGINE=InnoDB;
create index dataset_config_dataset_idx on dataset_config_index(dataset);
create index dataset_config_active_idx on dataset_config_index(active);
create index dataset_config_requires_completeness_check_idx on dataset_config_index(requires_completeness_check);

-- ALTER TABLE dataset_config_index ADD COLUMN requires_completeness_check boolean;
-- UPDATE dataset_config_index SET requires_completeness_check = 0;

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
    version int(10),
    CONSTRAINT uc_alert_name UNIQUE (name)
) ENGINE=InnoDB;
create index alert_confix_name_idx on alert_config_index(name);

create table if not exists data_completeness_config_index (
    dataset varchar(200) not null,
    date_to_check_in_ms bigint(20) not null,
    date_to_check_in_sdf varchar(20),
    data_complete boolean,
    percent_complete double,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `data_completeness_config_index` ADD UNIQUE `data_completeness_config_unique_index`(`dataset`, `date_to_check_in_ms`);
create index data_completeness_config_dataset_idx on data_completeness_config_index(dataset);
create index data_completeness_config_date_idx on data_completeness_config_index(date_to_check_in_ms);
create index data_completeness_config_sdf_idx on data_completeness_config_index(date_to_check_in_sdf);
create index data_completeness_config_complete_idx on data_completeness_config_index(data_complete);
create index data_completeness_config_percent_idx on data_completeness_config_index(percent_complete);

create table if not exists event_index (
  name VARCHAR (100),
  event_type VARCHAR (100),
  start_time bigint(20) not null,
  end_time bigint(20) not null,
  metric_name VARCHAR(200),
  service_name VARCHAR (200),
  base_id bigint(20) not null,
  create_time timestamp,
  update_time timestamp default current_timestamp,
  version int(10)
) ENGINE=InnoDB;
create index event_event_type_idx on event_index(event_type);
create index event_start_time_idx on event_index(start_time);
create index event_end_time_idx on event_index(end_time);

create table if not exists detection_status_index (
  function_id bigint(20),
  dataset varchar(200) not null,
  date_to_check_in_ms bigint(20) not null,
  date_to_check_in_sdf varchar(20),
  detection_run boolean,
  base_id bigint(20) not null,
  create_time timestamp,
  update_time timestamp default current_timestamp,
  version int(10)
) ENGINE=InnoDB;
ALTER TABLE `detection_status_index` ADD UNIQUE `detection_status_unique_index`(`function_id`, `date_to_check_in_sdf`);
create index detection_status_dataset_idx on detection_status_index(dataset);
create index detection_status_date_idx on detection_status_index(date_to_check_in_ms);
create index detection_status_sdf_idx on detection_status_index(date_to_check_in_sdf);
create index detection_status_run_idx on detection_status_index(detection_run);
create index detection_status_function_idx on detection_status_index(function_id);

create table if not exists autotune_config_index (
    function_id bigint(20),
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    performance_evaluation_method varchar(200),
    autotune_method varchar(200),
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index autotune_config_function_idx on autotune_config_index(function_id);
create index autotune_config_autoTuneMethod_idx on autotune_config_index(autotune_method);
create index autotune_config_performanceEval_idx on autotune_config_index(performance_evaluation_method);
create index autotune_config_start_time_idx on autotune_config_index(start_time);

create table if not exists classification_config_index (
    name varchar(200) not null,
    active boolean,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `classification_config_index` ADD UNIQUE `classification_config_unique_index`(`name`);
create index classification_config_name_index on classification_config_index(name);


create table if not exists entity_to_entity_mapping_index (
    from_urn varchar(500) not null,
    to_urn varchar(500) not null,
    mapping_type varchar(500) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `entity_to_entity_mapping_index` ADD UNIQUE `entity_mapping_unique_index`(`from_urn`, `to_urn`);
create index entity_mapping_from_urn_idx on entity_to_entity_mapping_index(from_urn);
create index entity_mapping_to_urn_idx on entity_to_entity_mapping_index(to_urn);
create index entity_mapping_type_idx on entity_to_entity_mapping_index(mapping_type);

create table if not exists grouped_anomaly_results_index (
    alert_config_id bigint(20) not null,
    dimensions varchar(1023),
    end_time bigint(20) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
create index grouped_anomaly_results_alert_config_id on grouped_anomaly_results_index(alert_config_id);

create table if not exists onboard_dataset_metric_index (
  dataset_name varchar(200) not null,
  metric_name varchar(500),
  data_source varchar(500) not null,
  onboarded boolean,
  base_id bigint(20) not null,
  create_time timestamp,
  update_time timestamp default current_timestamp,
  version int(10)
) ENGINE=InnoDB;
create index onboard_dataset_idx on onboard_dataset_metric_index(dataset_name);
create index onboard_metric_idx on onboard_dataset_metric_index(metric_name);
create index onboard_datasource_idx on onboard_dataset_metric_index(data_source);
create index onboard_onboarded_idx on onboard_dataset_metric_index(onboarded);

create table if not exists config_index (
    namespace varchar(64) not null,
    name varchar(128) not null,
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;
ALTER TABLE `config_index` ADD UNIQUE `config_unique_index`(`namespace`, `name`);
create index config_namespace_idx on config_index(namespace);
create index config_name_idx on config_index(name);

create table application_index (
  application VARCHAR (200) not null,
  recipients VARCHAR(1000) NOT NULL,
  base_id bigint(20) not null
);

create table if not exists alert_snapshot_index (
    base_id bigint(20) not null,
    create_time timestamp,
    update_time timestamp default current_timestamp,
    version int(10)
) ENGINE=InnoDB;

create table if not exists rootcause_session_index (
    base_id bigint(20) not null,
    create_time timestamp,
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
