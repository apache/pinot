package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Map;


public interface MergedAnomalyResultManager extends AbstractManager<MergedAnomalyResultDTO> {

  MergedAnomalyResultDTO findById(Long id);

  List<MergedAnomalyResultDTO> findByIdList(List<Long> idList);

  List<MergedAnomalyResultDTO> findOverlappingByFunctionId(long functionId, long conflictWindowStart,
      long conflictWindowEnd);

  List<MergedAnomalyResultDTO> findOverlappingByFunctionIdDimensions(long functionId, long conflictWindowStart, long conflictWindowEnd, String dimensions);

  List<MergedAnomalyResultDTO> findByCollectionMetricDimensionsTime(String collection, String metric, String dimensions,
      long startTime, long endTime);

  List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection, String metric, long startTime,
      long endTime);

  List<MergedAnomalyResultDTO> findByMetricTime(String metric, long startTime, long endTime);

  List<MergedAnomalyResultDTO> findByDetectionConfigAndIdGreaterThan(Long detectionConfigId, Long anomalyId);

  // TODO : add findByMetricId - currently we are not updating metricId in table.

  List<MergedAnomalyResultDTO> findByCollectionTime(String collection, long startTime, long endTime);

  MergedAnomalyResultDTO findLatestOverlapByFunctionIdDimensions(Long functionId, String dimensions,
      long conflictWindowStart, long conflictWindowEnd);

  List<MergedAnomalyResultDTO> findByFunctionId(Long functionId);

  List<MergedAnomalyResultDTO> findByFunctionIdAndIdGreaterThan(Long functionId, Long anomalyId);

  List<MergedAnomalyResultDTO> findByStartTimeInRangeAndFunctionId(long startTime, long endTime, long functionId);

  List<MergedAnomalyResultDTO> findByTime(long startTime, long endTime);

  List<MergedAnomalyResultDTO> findUnNotifiedByFunctionIdAndIdLesserThanAndEndTimeGreaterThanLastOneDay(long functionId,
      long anomalyId);

  List<MergedAnomalyResultDTO> findNotifiedByTime(long startTime, long endTime);

  Map<Long, List<MergedAnomalyResultDTO>> findAnomaliesByMetricIdsAndTimeRange(List<Long> metricIds, long start, long end);

  List<MergedAnomalyResultDTO> findAnomaliesByMetricIdAndTimeRange(Long metricId, long start, long end);

  void updateAnomalyFeedback(MergedAnomalyResultDTO entity);

  MergedAnomalyResultBean convertMergeAnomalyDTO2Bean(MergedAnomalyResultDTO entity);

  MergedAnomalyResultDTO convertMergedAnomalyBean2DTO(MergedAnomalyResultBean mergedAnomalyResultBean);

  List<MergedAnomalyResultDTO> convertMergedAnomalyBean2DTO(List<MergedAnomalyResultBean> mergedAnomalyResultBeanList);
}
