-- drop database thirdeye; create database thirdeye; use thirdeye;

create table if not exists metric (
    id bigint(20) primary key auto_increment,
    name varchar(200) not null,
    collection varchar(200) not null,
    derived boolean default false,
    dimension_as_metric boolean default false,
    json_val JSON
) ENGINE=InnoDB;

create table if not exists anomaly_function (
    id bigint(20) primary key auto_increment,
    name varchar(200) not null,
    active boolean,
    metric_id bigint(20) not null,
    collection varchar(200),
    metric varchar(200),
    json_val JSON,
    foreign key (metric_id) references metric(id)
) ENGINE=InnoDB;

create table if not exists anomaly_merge_config (
    id bigint(20) primary key auto_increment,
    name varchar(200) not null,
    json_val JSON
) ENGINE=InnoDB;

create table if not exists anomaly_function_merge_config_mapping(
    anomaly_function_id bigint(20),
    anomaly_merge_config_id bigint(20),
    foreign key (anomaly_function_id) references anomaly_function(id),
    foreign key (anomaly_merge_config_id) references anomaly_merge_config(id)
) ENGINE=InnoDB;

create table if not exists email_configuration (
    id bigint(20) primary key auto_increment,
    name varchar(200) not null,
    active boolean,
    json_val JSON
) ENGINE=InnoDB;

create table if not exists email_function_mapping (
    email_configuration_id bigint(20),
    anomaly_function_id bigint(20),
    foreign key (email_configuration_id) references email_configuration(id),
    foreign key (anomaly_function_id) references anomaly_function(id)
) ENGINE=InnoDB;

create table if not exists job (
    id bigint(20) primary key auto_increment,
    name varchar(200) not null,
    status varchar(100) not null,
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    json_val JSON
) ENGINE=InnoDB;

create table if not exists task (
    id bigint(20) primary key auto_increment,
    name varchar(200) not null,
    status varchar(100) not null,
    type varchar(100) not null,
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    job_id bigint(20),
    worker_id bigint(20),
    version integer,
    json_val JSON,
    foreign key (job_id) references job(id)
) ENGINE=InnoDB;

create table if not exists anomaly_feedback (
    id bigint(20) primary key auto_increment,
    type varchar(100) not null,
    json_val JSON
) ENGINE=InnoDB;

create table if not exists anomaly_raw_result (
    id bigint(20) primary key auto_increment,
    anomaly_function_id bigint(20),
    job_id bigint(20),
    anomaly_feedback_id bigint(20),
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    data_missing boolean default false not null,
    merged boolean default false,
    dimension_value varchar(1023),
    json_val JSON,
    foreign key (anomaly_function_id) references anomaly_function(id),
    foreign key (anomaly_feedback_id) references anomaly_feedback(id),
    foreign key (job_id) references job(id)
) ENGINE=InnoDB;

create table if not exists anomaly_merged_result (
    id bigint(20) primary key auto_increment,
    anomaly_function_id bigint(20),
    anomaly_feedback_id bigint(20),
    metric_id bigint(20),
    start_time bigint(20) not null,
    end_time bigint(20) not null,
    collection varchar(200),
    metric varchar(200),
    dimension_value varchar(1023),
    notified boolean default false,
    json_val JSON,
    foreign key (anomaly_function_id) references anomaly_function(id),
    foreign key (anomaly_feedback_id) references anomaly_feedback(id),
    foreign key (metric_id) references metric(id)
) ENGINE=InnoDB;

create table if not exists anomaly_raw_merged_result_mapping (
    anomaly_raw_result_id bigint(20),
    anomaly_merged_result_id bigint(20),
    foreign key (anomaly_raw_result_id) references anomaly_raw_result(id),
    foreign key (anomaly_merged_result_id) references anomaly_merged_result(id)
) ENGINE=InnoDB;

create table if not exists ingraph_metric_config (
    id bigint(20) primary key auto_increment,
    name varchar(200) not null,
    alias varchar(200) not null,
    json_val JSON
) ENGINE=InnoDB;

create table if not exists webapp_config (
    id bigint(20) primary key auto_increment,
    name varchar(200) not null,
    collection varchar(200) not null,
    type varchar(100) not null,
    json_val JSON
) ENGINE=InnoDB;
