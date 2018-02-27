package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class MergedAnomalyResultManagerImpl extends AbstractManagerImpl<MergedAnomalyResultDTO>
    implements MergedAnomalyResultManager {
  private static final Logger LOG = LoggerFactory.getLogger(MergedAnomalyResultManagerImpl.class);

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME =
      " where collection=:collection and metric=:metric " + "and dimensions in (:dimensions) "
          + "and (startTime < :endTime and endTime > :startTime) " + "order by endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_METRIC_TIME =
      "where collection=:collection and metric=:metric "
          + "and (startTime < :endTime and endTime > :startTime) order by endTime desc";

  // find a conflicting window
  private static final String FIND_BY_METRIC_TIME =
      "where metric=:metric and (startTime < :endTime and endTime > :startTime) order by endTime desc";


  // find a conflicting window
  private static final String FIND_BY_COLLECTION_TIME = "where collection=:collection "
      + "and (startTime < :endTime and endTime > :startTime) order by endTime desc";

  private static final String FIND_BY_TIME = "where (startTime < :endTime and endTime > :startTime) "
      + "order by endTime desc";

  private static final String FIND_BY_FUNCTION_ID = "where functionId=:functionId";

  private static final String FIND_LATEST_CONFLICT_BY_FUNCTION_AND_DIMENSIONS =
      "where functionId=:functionId " + "and dimensions=:dimensions "
        + "and (startTime < :endTime and endTime > :startTime) " + "order by endTime desc limit 1";

  private static final String FIND_BY_FUNCTION_AND_NULL_DIMENSION =
      "where functionId=:functionId " + "and dimensions is null order by endTime desc";

  // TODO inject as dependency
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(10);

  public MergedAnomalyResultManagerImpl() {
    super(MergedAnomalyResultDTO.class, MergedAnomalyResultBean.class);
  }

  public Long save(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
    if (mergedAnomalyResultDTO.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(mergedAnomalyResultDTO);
      return mergedAnomalyResultDTO.getId();
    }
    MergedAnomalyResultBean mergeAnomalyBean = convertMergeAnomalyDTO2Bean(mergedAnomalyResultDTO);
    Long id = genericPojoDao.put(mergeAnomalyBean);
    mergedAnomalyResultDTO.setId(id);
    return id;
  }

  public int update(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
    if (mergedAnomalyResultDTO.getId() == null) {
      Long id = save(mergedAnomalyResultDTO);
      if (id > 0) {
        return 1;
      } else {
        return 0;
      }
    } else {
      MergedAnomalyResultBean mergeAnomalyBean = convertMergeAnomalyDTO2Bean(mergedAnomalyResultDTO);
      return genericPojoDao.update(mergeAnomalyBean);
    }
  }

  public MergedAnomalyResultDTO findById(Long id, boolean loadRawAnomalies) {
    MergedAnomalyResultBean mergedAnomalyResultBean = genericPojoDao.get(id, MergedAnomalyResultBean.class);
    if (mergedAnomalyResultBean != null) {
      MergedAnomalyResultDTO mergedAnomalyResultDTO;
      mergedAnomalyResultDTO = convertMergedAnomalyBean2DTO(mergedAnomalyResultBean, loadRawAnomalies);
      return mergedAnomalyResultDTO;
    } else {
      return null;
    }
  }

  public List<MergedAnomalyResultDTO> findByIdList(List<Long> idList, boolean loadRawAnomalies) {
    List<MergedAnomalyResultBean> mergedAnomalyResultBeanList =
        genericPojoDao.get(idList, MergedAnomalyResultBean.class);
    if (CollectionUtils.isNotEmpty(mergedAnomalyResultBeanList)) {
      List<MergedAnomalyResultDTO> mergedAnomalyResultDTOList =
          convertMergedAnomalyBean2DTO(mergedAnomalyResultBeanList, loadRawAnomalies);
      return mergedAnomalyResultDTOList;
    } else {
      return Collections.emptyList();
    }
  }

  public MergedAnomalyResultDTO findById(Long id) {
    return findById(id, true);
  }

  public List<MergedAnomalyResultDTO> findByIdList(List<Long> idList) {
    return findByIdList(idList, true);
  }

  @Override
  public List<MergedAnomalyResultDTO> findOverlappingByFunctionId(long functionId, long searchWindowStart,
      long searchWindowEnd, boolean loadRawAnomalies) {
    // LT and GT are used instead of LE and GE because ThirdEye uses end time exclusive.
    Predicate predicate = Predicate
        .AND(Predicate.LT("startTime", searchWindowEnd), Predicate.GT("endTime", searchWindowStart),
            Predicate.EQ("functionId", functionId));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }

  @Override
  public List<MergedAnomalyResultDTO> findOverlappingByFunctionIdDimensions(long functionId, long searchWindowStart,
      long searchWindowEnd, String dimensions, boolean loadRawAnomalies) {
    // LT and GT are used instead of LE and GE because ThirdEye uses end time exclusive.
    Predicate predicate = Predicate
        .AND(Predicate.LT("startTime", searchWindowEnd), Predicate.GT("endTime", searchWindowStart),
            Predicate.EQ("functionId", functionId), Predicate.EQ("dimensions", dimensions));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByFunctionId(Long functionId, boolean loadRawAnomalies) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(FIND_BY_FUNCTION_ID,
        filterParams, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByFunctionIdAndIdGreaterThan(Long functionId, Long anomalyId,
      boolean loadRawAnomalies) {
    Predicate predicate = Predicate.AND(Predicate.EQ("functionId", functionId), Predicate.GT("baseId", anomalyId));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }


  @Override
  public List<MergedAnomalyResultDTO> findByStartTimeInRangeAndFunctionId(long startTime, long
      endTime, long functionId, boolean loadRawAnomalies) {
    Predicate predicate =
        Predicate.AND(Predicate.GE("startTime", startTime), Predicate.LT("endTime", endTime),
            Predicate.EQ("functionId", functionId));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByCollectionMetricDimensionsTime(String collection,
      String metric, String dimensions, long startTime, long endTime, boolean loadRawAnomalies) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("metric", metric);
    filterParams.put("dimensions", dimensions);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME, filterParams, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection, String metric,
      long startTime, long endTime, boolean loadRawAnomalies) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("metric", metric);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_COLLECTION_METRIC_TIME, filterParams, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }

  public List<MergedAnomalyResultDTO> findByMetricTime(String metric, long startTime, long endTime, boolean loadRawAnomalies) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("metric", metric);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_METRIC_TIME, filterParams, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByCollectionTime(String collection, long startTime,
      long endTime, boolean loadRawAnomalies) {
    Predicate predicate = Predicate
        .AND(Predicate.EQ("collection", collection), Predicate.GT("startTime", startTime),
            Predicate.LT("endTime", endTime));

    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByTime(long startTime, long endTime, boolean loadRawAnomalies) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_TIME, filterParams, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }

  public List<MergedAnomalyResultDTO> findUnNotifiedByFunctionIdAndIdLesserThanAndEndTimeGreaterThanLastOneDay(long functionId, long anomalyId, boolean loadRawAnomalies) {
    Predicate predicate = Predicate
        .AND(Predicate.EQ("functionId", functionId), Predicate.LT("baseId", anomalyId),
            Predicate.EQ("notified", false), Predicate.GT("endTime", System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1)));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
  }

  @Override
  public List<MergedAnomalyResultDTO> findNotifiedByTime(long startTime, long endTime, boolean loadRawAnomalies) {
    List<MergedAnomalyResultDTO> anomaliesByTime = findByTime(startTime, endTime, loadRawAnomalies);
    List<MergedAnomalyResultDTO> notifiedAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : anomaliesByTime) {
      if (anomaly.isNotified()) {
        notifiedAnomalies.add(anomaly);
      }
    }
    return notifiedAnomalies;
  }

  @Override
  public MergedAnomalyResultDTO findLatestOverlapByFunctionIdDimensions(Long functionId, String dimensions,
      long conflictWindowStart, long conflictWindowEnd, boolean loadRawAnomalies) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);
    filterParams.put("dimensions", dimensions);
    filterParams.put("startTime", conflictWindowStart);
    filterParams.put("endTime", conflictWindowEnd);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_LATEST_CONFLICT_BY_FUNCTION_AND_DIMENSIONS, filterParams, MergedAnomalyResultBean.class);

    if (CollectionUtils.isNotEmpty(list)) {
      MergedAnomalyResultBean mostRecentConflictMergedAnomalyResultBean = list.get(0);
      return convertMergedAnomalyBean2DTO(mostRecentConflictMergedAnomalyResultBean, loadRawAnomalies);
    }
    return null;
  }

  @Override
  public MergedAnomalyResultDTO findLatestByFunctionIdOnly(Long functionId, boolean loadRawAnomalies) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_FUNCTION_AND_NULL_DIMENSION, filterParams, MergedAnomalyResultBean.class);
    List<MergedAnomalyResultDTO> result = convertMergedAnomalyBean2DTO(list, loadRawAnomalies);
    // TODO: Check list size instead of result size?
    if (result.size() > 0) {
      return result.get(0);
    }
    return null;
  }

  public void updateAnomalyFeedback(MergedAnomalyResultDTO entity) {
    MergedAnomalyResultBean bean = convertDTO2Bean(entity, MergedAnomalyResultBean.class);
    AnomalyFeedbackDTO feedbackDTO = (AnomalyFeedbackDTO) entity.getFeedback();
    if (feedbackDTO != null) {
      if (feedbackDTO.getId() == null) {
        AnomalyFeedbackBean feedbackBean = convertDTO2Bean(feedbackDTO, AnomalyFeedbackBean.class);
        Long feedbackId = genericPojoDao.put(feedbackBean);
        feedbackDTO.setId(feedbackId);
      } else {
        AnomalyFeedbackBean feedbackBean = genericPojoDao.get(feedbackDTO.getId(), AnomalyFeedbackBean.class);
        feedbackBean.setFeedbackType(feedbackDTO.getFeedbackType());
        feedbackBean.setComment(feedbackDTO.getComment());
        genericPojoDao.update(feedbackBean);
      }
      bean.setAnomalyFeedbackId(feedbackDTO.getId());
    }
    genericPojoDao.update(bean);
  }

  /**
   * Returns a map (keyed by metric id) of lists of merged anomalies that fall (partially)
   * within a given time range.
   *
   * <br/><b>NOTE:</b> this function implements a manual join between three tables. This is bad.
   *
   * @param metricIds metric ids to seek anomalies for
   * @param start time range start (inclusive)
   * @param end time range end (exclusive)
   * @return Map (keyed by metric id) of lists of merged anomalies (sorted by start time)
   */
  @Override
  public Map<Long, List<MergedAnomalyResultDTO>> findAnomaliesByMetricIdsAndTimeRange(List<Long> metricIds, long start, long end) {
    Map<Long, List<MergedAnomalyResultDTO>> output = new HashMap<>();

    List<MetricConfigBean> metricBeans = genericPojoDao.get(metricIds, MetricConfigBean.class);

    for (MetricConfigBean mbean : metricBeans) {
      output.put(mbean.getId(), getAnomaliesForMetricBeanAndTimeRange(mbean, start, end));
    }

    return output;
  }

  /**
   * Returns a list of merged anomalies that fall (partially) within a given time range for
   * a given metric id
   *
   * <br/><b>NOTE:</b> this function implements a manual join between three tables. This is bad.
   *
   * @param metricId metric id to seek anomalies for
   * @param start time range start (inclusive)
   * @param end time range end (exclusive)
   * @return List of merged anomalies (sorted by start time)
   */
  @Override
  public List<MergedAnomalyResultDTO> findAnomaliesByMetricIdAndTimeRange(Long metricId, long start, long end) {
    MetricConfigBean mbean = genericPojoDao.get(metricId, MetricConfigBean.class);
    if (mbean == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id '%d'", metricId));
    }

    return this.getAnomaliesForMetricBeanAndTimeRange(mbean, start, end);
  }

  private List<MergedAnomalyResultDTO> getAnomaliesForMetricBeanAndTimeRange(MetricConfigBean mbean, long start, long end) {

    LOG.info("Fetching functions for metricId={}", mbean.getId());

    // TODO use metricId instead of metric/dataset on join (requires database cleanup)
    List<AnomalyFunctionBean> functionBeans = genericPojoDao.get(
        Predicate.AND(
            Predicate.EQ("metric", mbean.getName()),
            Predicate.EQ("collection", mbean.getDataset())),
        AnomalyFunctionBean.class);

    // extract functionIds
    List<Long> functionIds = new ArrayList<>();
    for(AnomalyFunctionBean fbean : functionBeans) {
      functionIds.add(fbean.getId());
    }

    LOG.info("Fetching anomalies for metricId={}, functionIds={}", mbean.getId(), functionIds);

    List<MergedAnomalyResultBean> anomalyBeans = genericPojoDao.get(
        Predicate.AND(
            Predicate.IN("functionId", functionIds.toArray()),
            Predicate.LT("startTime", end),
            Predicate.GT("endTime", start)
        ),
        MergedAnomalyResultBean.class);

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>(
        convertMergedAnomalyBean2DTO(anomalyBeans, false));

    Collections.sort(anomalies, new Comparator<MergedAnomalyResultDTO>() {
      @Override
      public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
        return Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    });

    return anomalies;
  }

  @Override
  public MergedAnomalyResultBean convertMergeAnomalyDTO2Bean(MergedAnomalyResultDTO entity) {
    MergedAnomalyResultBean bean = convertDTO2Bean(entity, MergedAnomalyResultBean.class);
    AnomalyFeedbackDTO feedbackDTO = (AnomalyFeedbackDTO) entity.getFeedback();
    if (feedbackDTO != null && feedbackDTO.getId() != null) {
        bean.setAnomalyFeedbackId(feedbackDTO.getId());
    }

    if (entity.getFunction() != null) {
      bean.setFunctionId(entity.getFunction().getId());
    }

    if (entity.getAnomalyResults() != null && !entity.getAnomalyResults().isEmpty()) {
      List<Long> rawAnomalyIds = new ArrayList<>();
      for (RawAnomalyResultDTO rawAnomalyDTO : entity.getAnomalyResults()) {
        rawAnomalyIds.add(rawAnomalyDTO.getId());
      }
      bean.setRawAnomalyIdList(rawAnomalyIds);
    }
    return bean;
  }

  @Override
  public MergedAnomalyResultDTO convertMergedAnomalyBean2DTO(MergedAnomalyResultBean mergedAnomalyResultBean,
      boolean loadRawAnomalies) {
    MergedAnomalyResultDTO mergedAnomalyResultDTO;
    mergedAnomalyResultDTO =
        MODEL_MAPPER.map(mergedAnomalyResultBean, MergedAnomalyResultDTO.class);
    if (mergedAnomalyResultBean.getFunctionId() != null) {
      AnomalyFunctionBean anomalyFunctionBean =
          genericPojoDao.get(mergedAnomalyResultBean.getFunctionId(), AnomalyFunctionBean.class);
      AnomalyFunctionDTO anomalyFunctionDTO =
          MODEL_MAPPER.map(anomalyFunctionBean, AnomalyFunctionDTO.class);
      mergedAnomalyResultDTO.setFunction(anomalyFunctionDTO);
    }
    if (mergedAnomalyResultBean.getAnomalyFeedbackId() != null) {
      AnomalyFeedbackBean anomalyFeedbackBean = genericPojoDao
          .get(mergedAnomalyResultBean.getAnomalyFeedbackId(), AnomalyFeedbackBean.class);
      AnomalyFeedbackDTO anomalyFeedbackDTO =
          MODEL_MAPPER.map(anomalyFeedbackBean, AnomalyFeedbackDTO.class);
      mergedAnomalyResultDTO.setFeedback(anomalyFeedbackDTO);
    }
    if (loadRawAnomalies && mergedAnomalyResultBean.getRawAnomalyIdList() != null
        && !mergedAnomalyResultBean.getRawAnomalyIdList().isEmpty()) {
      List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();
      List<RawAnomalyResultBean> list = genericPojoDao
          .get(mergedAnomalyResultBean.getRawAnomalyIdList(), RawAnomalyResultBean.class);
      for (RawAnomalyResultBean rawAnomalyResultBean : list) {
        anomalyResults.add(createRawAnomalyDTOFromBean(rawAnomalyResultBean));
      }
      mergedAnomalyResultDTO.setAnomalyResults(anomalyResults);
    }

    return mergedAnomalyResultDTO;
  }

  @Override
  public List<MergedAnomalyResultDTO> convertMergedAnomalyBean2DTO(
      List<MergedAnomalyResultBean> mergedAnomalyResultBeanList, final boolean loadRawAnomalies) {
    List<Future<MergedAnomalyResultDTO>> mergedAnomalyResultDTOFutureList = new ArrayList<>(mergedAnomalyResultBeanList.size());
    for (final MergedAnomalyResultBean mergedAnomalyResultBean : mergedAnomalyResultBeanList) {
      Future<MergedAnomalyResultDTO> future =
          EXECUTOR_SERVICE.submit(new Callable<MergedAnomalyResultDTO>() {
            @Override public MergedAnomalyResultDTO call() throws Exception {
              return MergedAnomalyResultManagerImpl.this
                  .convertMergedAnomalyBean2DTO(mergedAnomalyResultBean, loadRawAnomalies);
            }
          });
      mergedAnomalyResultDTOFutureList.add(future);
    }

    List<MergedAnomalyResultDTO> mergedAnomalyResultDTOList = new ArrayList<>(mergedAnomalyResultBeanList.size());
    for (Future future : mergedAnomalyResultDTOFutureList) {
      try {
        mergedAnomalyResultDTOList.add((MergedAnomalyResultDTO) future.get(60, TimeUnit.SECONDS));
      } catch (InterruptedException | TimeoutException | ExecutionException e) {
        LOG.warn("Failed to convert MergedAnomalyResultDTO from bean: {}", e.toString());
      }
    }

    return mergedAnomalyResultDTOList;
  }

  @Override
  public List<MergedAnomalyResultDTO> findByPredicate(Predicate predicate) {
    List<MergedAnomalyResultDTO> dtoList = super.findByPredicate(predicate);
    List<MergedAnomalyResultBean> beanList = new ArrayList<>();
    for (MergedAnomalyResultDTO mergedAnomalyResultDTO :  dtoList) {
      beanList.add(mergedAnomalyResultDTO);
    }
    return convertMergedAnomalyBean2DTO(beanList, true);
  }
}
