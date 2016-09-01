package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.AnomalyMergedResultDTO;
import com.linkedin.thirdeye.datalayer.entity.AnomalyMergedResult;
import java.util.Arrays;
import java.util.List;

public class AnomalyMergedResultManager extends AbstractManager<AnomalyMergedResultDTO, AnomalyMergedResult> {

  public List<AnomalyMergedResult> getAllByTime(long startTime, long endTime) {
    return null;
  }

  public List<AnomalyMergedResult> getAllByTimeEmailIdAndNotifiedFalse(long startTime, long endTime, long emailId) {
    return null;
  }

  public List<AnomalyMergedResult> findByCollectionMetricDimensionsTime(String collection,
      String metric, String [] dimensions, long startTime, long endTime) {
    List<String> dimList = Arrays.asList(dimensions);
    return null;
  }

  public List<AnomalyMergedResult> findByCollectionMetricTime(String collection,
      String metric, long startTime, long endTime) {
    return null;
  }

  public List<AnomalyMergedResult> findByCollectionTime(String collection,
      long startTime, long endTime) {
    return null;
  }

  public AnomalyMergedResult findLatestByCollectionMetricDimensions(
      String collection, String metric, String dimensions) {
    return null;
  }

  public AnomalyMergedResult findLatestByFunctionIdDimensions(Long functionId, String dimensions) {
    return null;
  }

  public AnomalyMergedResult findLatestByFunctionIdOnly(Long functionId) {
    return null;
  }
}
