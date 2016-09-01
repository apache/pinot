package com.linkedin.thirdeye.datalayer.dao;

import com.linkedin.thirdeye.datalayer.entity.Task;

public class TaskDAO extends AbstractBaseDAO<Task> {

  public TaskDAO() {
    super(Task.class);
  }
}
