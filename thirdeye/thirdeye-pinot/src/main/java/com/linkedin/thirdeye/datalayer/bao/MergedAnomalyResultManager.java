package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Map;


public interface MergedAnomalyResultManager extends AbstractManager<MergedAnomalyResultDTO> {

  MergedAnomalyResultDTO findById(Long id, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findByIdList(List<Long> idList);

  List<MergedAnomalyResultDTO> findByIdList(List<Long> idList, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findOverlappingByFunctionId(long functionId, long conflictWindowStart, long conflictWindowEnd, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findOverlappingByFunctionIdDimensions(long functionId, long conflictWindowStart, long conflictWindowEnd, String dimensions, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findByCollectionMetricDimensionsTime(String collection,
      String metric, String dimensions, long startTime, long endTime, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection, String metric, long startTime, long endTime, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findByMetricTime(String metric, long startTime, long endTime, boolean loadRawAnomalies);


  // TODO : add findByMetricId - currently we are not updating metricId in table.

  List<MergedAnomalyResultDTO> findByCollectionTime(String collection, long startTime,
      long endTime, boolean loadRawAnomalies);

  MergedAnomalyResultDTO findLatestOverlapByFunctionIdDimensions(Long functionId, String dimensions,
      long conflictWindowStart, long conflictWindowEnd, boolean loadRawAnomalies);

  MergedAnomalyResultDTO findLatestByFunctionIdOnly(Long functionId, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findByFunctionId(Long functionId, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findByFunctionIdAndIdGreaterThan(Long functionId, Long anomalyId, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findByStartTimeInRangeAndFunctionId(long startTime, long endTime,
      long functionId, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findByTime(long startTime, long endTime, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findUnNotifiedByFunctionIdAndIdLesserThanAndEndTimeGreaterThanLastOneDay(long functionId, long anomalyId, boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> findNotifiedByTime(long startTime, long endTime, boolean loadRawAnomalies);

  Map<Long, List<MergedAnomalyResultDTO>> findAnomaliesByMetricIdsAndTimeRange(List<Long> metricIds, long start, long end);

  List<MergedAnomalyResultDTO> findAnomaliesByMetricIdAndTimeRange(Long metricId, long start, long end);

  void updateAnomalyFeedback(MergedAnomalyResultDTO entity);

  MergedAnomalyResultBean convertMergeAnomalyDTO2Bean(MergedAnomalyResultDTO entity);

  MergedAnomalyResultDTO convertMergedAnomalyBean2DTO(MergedAnomalyResultBean mergedAnomalyResultBean,
      boolean loadRawAnomalies);

  List<MergedAnomalyResultDTO> convertMergedAnomalyBean2DTO(
      List<MergedAnomalyResultBean> mergedAnomalyResultBeanList, boolean loadRawAnomalies);
}
