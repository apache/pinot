package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public interface MergedAnomalyResultManager extends AbstractManager<MergedAnomalyResultDTO> {

  List<MergedAnomalyResultDTO> getAllByTimeEmailIdAndNotifiedFalse(long startTime, long endTime,
      long emailId);

  List<MergedAnomalyResultDTO> findAllConflictByFunctionId(long functionId, long conflictWindowStart, long conflictWindowEnd);

  List<MergedAnomalyResultDTO> findByCollectionMetricDimensionsTime(String collection,
      String metric, String dimensions, long startTime, long endTime);

  List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection, String metric,
      long startTime, long endTime);

  List<MergedAnomalyResultDTO> findByCollectionTime(String collection, long startTime,
      long endTime);

  MergedAnomalyResultDTO findLatestByFunctionIdDimensions(Long functionId, String dimensions);

  MergedAnomalyResultDTO findLatestConflictByFunctionIdDimensions(Long functionId, String dimensions, long conflictWindowStart, long conflictWindowEnd);

  MergedAnomalyResultDTO findLatestByFunctionIdOnly(Long functionId);

  List<MergedAnomalyResultDTO> findByFunctionId(Long functionId);
}
