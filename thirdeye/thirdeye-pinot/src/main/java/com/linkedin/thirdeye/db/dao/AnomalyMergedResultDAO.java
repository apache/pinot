package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnomalyMergedResultDAO extends AbstractJpaDAO<AnomalyMergedResult> {

  public AnomalyMergedResultDAO() {
    super(AnomalyMergedResult.class);
  }

  public List<AnomalyMergedResult> findByCollectionMetricDimensions(String collection,
      String metric, String dimensions) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("collection", collection);
    filters.put("metric", metric);
    filters.put("dimensions", dimensions);
    return super.findByParams(filters);
  }
}
