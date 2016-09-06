package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.api.dto.GroupByKey;
import com.linkedin.thirdeye.api.dto.GroupByRow;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;


public interface RawAnomalyResultManager extends AbstractManager<RawAnomalyResultDTO> {

  List<RawAnomalyResultDTO> findAllByTimeAndFunctionId(long startTime, long endTime,
      long functionId);

  List<RawAnomalyResultDTO> findAllByTimeFunctionIdAndDimensions(long startTime, long endTime,
      long functionId, String dimensions);

  List<GroupByRow<GroupByKey, Long>> getCountByFunction(long startTime, long endTime);

  List<GroupByRow<GroupByKey, Long>> getCountByFunctionDimensions(long startTime, long endTime);

  List<RawAnomalyResultDTO> findUnmergedByFunctionId(Long functionId);

  List<RawAnomalyResultDTO> findUnmergedByCollectionMetricAndDimensions(String collection,
      String metric, String dimensions);

}
