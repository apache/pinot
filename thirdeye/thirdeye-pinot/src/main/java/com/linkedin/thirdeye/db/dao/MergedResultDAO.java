package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;

public class MergedResultDAO extends AbstractJpaDAO<AnomalyMergedResult> {
  public MergedResultDAO() {
    super(AnomalyMergedResult.class);
  }
}
