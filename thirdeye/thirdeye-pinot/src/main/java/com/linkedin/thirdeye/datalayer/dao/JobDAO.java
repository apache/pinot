package com.linkedin.thirdeye.datalayer.dao;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.entity.Job;

public class JobDAO extends AbstractBaseDAO<Job> {

  public JobDAO() {
    super(Job.class);
  }
}
