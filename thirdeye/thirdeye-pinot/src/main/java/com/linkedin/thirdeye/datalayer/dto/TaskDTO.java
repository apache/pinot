package com.linkedin.thirdeye.datalayer.dto;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.linkedin.thirdeye.datalayer.pojo.TaskBean;


/**
 * This class corresponds to anomaly tasks. An execution of an anomaly function creates an anomaly
 * job, which in turn spawns into 1 or more anomaly tasks. The anomaly tasks are picked by the
 * workers
 */
@Entity
@Table(name = "anomaly_tasks")

public class TaskDTO extends TaskBean {

  @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER, optional = true)
  @JoinColumn(name = "job_id")
  private JobDTO job;

  public JobDTO getJob() {
    return job;
  }

  public void setJob(JobDTO job) {
    this.job = job;
  }
}
