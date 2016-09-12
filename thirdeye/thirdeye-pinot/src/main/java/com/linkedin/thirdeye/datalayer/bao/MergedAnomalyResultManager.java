package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public interface MergedAnomalyResultManager extends AbstractManager<MergedAnomalyResultDTO> {

  List<MergedAnomalyResultDTO> getAllByTimeEmailIdAndNotifiedFalse(long startTime, long endTime,
      long emailId);

  List<MergedAnomalyResultDTO> findByCollectionMetricDimensionsTime(String collection,
      String metric, String[] dimensions, long startTime, long endTime);

  List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection, String metric,
      long startTime, long endTime);

  List<MergedAnomalyResultDTO> findByCollectionTime(String collection, long startTime,
      long endTime);

  MergedAnomalyResultDTO findLatestByFunctionIdDimensions(Long functionId, String dimensions);

  MergedAnomalyResultDTO findLatestByFunctionIdOnly(Long functionId);

  List<MergedAnomalyResultDTO> findByFunctionId(Long functionId);
}
