package com.linkedin.thirdeye.datalayer.dto;


import com.linkedin.thirdeye.datalayer.pojo.TaskBean;


/**
 * This class corresponds to anomaly tasks. An execution of an anomaly function creates an anomaly
 * job, which in turn spawns into 1 or more anomaly tasks. The anomaly tasks are picked by the
 * workers
 */
public class TaskDTO extends TaskBean {

}
