/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.datalayer.bao.jdbc;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import org.apache.pinot.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import org.apache.pinot.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import org.apache.pinot.thirdeye.datalayer.pojo.MetricConfigBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
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

  @Inject
  public MergedAnomalyResultManagerImpl(GenericPojoDao genericPojoDao) {
    super(MergedAnomalyResultDTO.class, MergedAnomalyResultBean.class, genericPojoDao);
  }

  public Long save(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
    if (mergedAnomalyResultDTO.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(mergedAnomalyResultDTO);
      return mergedAnomalyResultDTO.getId();
    }
    return saveAnomaly(mergedAnomalyResultDTO, new HashSet<>());
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
      return updateAnomaly(mergedAnomalyResultDTO, new HashSet<>());
    }
  }

  private Long saveAnomaly(MergedAnomalyResultDTO mergedAnomalyResultDTO, Set<MergedAnomalyResultDTO> visitedAnomalies) {
    Preconditions.checkNotNull(mergedAnomalyResultDTO);
    Preconditions.checkNotNull(visitedAnomalies);

    visitedAnomalies.add(mergedAnomalyResultDTO);

    if (mergedAnomalyResultDTO.getId() != null) {
      updateAnomaly(mergedAnomalyResultDTO, visitedAnomalies);
      return mergedAnomalyResultDTO.getId();
    }

    MergedAnomalyResultBean mergeAnomalyBean = convertMergeAnomalyDTO2Bean(mergedAnomalyResultDTO);
    Set<Long> childAnomalyIds = saveChildAnomalies(mergedAnomalyResultDTO, visitedAnomalies);
    mergeAnomalyBean.setChildIds(childAnomalyIds);

    Long id = genericPojoDao.put(mergeAnomalyBean);
    mergedAnomalyResultDTO.setId(id);
    return id;
  }

  private int updateAnomaly(MergedAnomalyResultDTO mergedAnomalyResultDTO, Set<MergedAnomalyResultDTO> visitedAnomalies) {
    visitedAnomalies.add(mergedAnomalyResultDTO);

    if (mergedAnomalyResultDTO.getId() == null) {
      Long id = saveAnomaly(mergedAnomalyResultDTO, visitedAnomalies);
      if (id > 0) {
        return 1;
      } else {
        return 0;
      }
    }

    MergedAnomalyResultBean mergeAnomalyBean = convertMergeAnomalyDTO2Bean(mergedAnomalyResultDTO);
    Set<Long> childAnomalyIds = saveChildAnomalies(mergedAnomalyResultDTO, visitedAnomalies);
    mergeAnomalyBean.setChildIds(childAnomalyIds);

    return genericPojoDao.update(mergeAnomalyBean);
  }

  private Set<Long> saveChildAnomalies(MergedAnomalyResultDTO parentAnomaly,
      Set<MergedAnomalyResultDTO> visitedAnomalies) {
    Set<Long> childIds = new HashSet<>();
    Set<MergedAnomalyResultDTO> childAnomalies = parentAnomaly.getChildren();
    if (childAnomalies == null || childAnomalies.isEmpty()) {
      // No child anomalies to save
      return childIds;
    }

    for (MergedAnomalyResultDTO child : childAnomalies) {
      if (child.getId() == null) {
        // Prevent cycles
        if (visitedAnomalies.contains(child)) {
          throw new IllegalArgumentException("Loop detected! Child anomaly referencing ancestor");
        }
      }
      child.setChild(true);
      childIds.add(saveAnomaly(child, visitedAnomalies));
    }

    return childIds;
  }

  @Override
  public MergedAnomalyResultDTO findById(Long id) {
    MergedAnomalyResultBean mergedAnomalyResultBean = genericPojoDao.get(id, MergedAnomalyResultBean.class);
    if (mergedAnomalyResultBean != null) {
      MergedAnomalyResultDTO mergedAnomalyResultDTO;
      mergedAnomalyResultDTO = convertMergedAnomalyBean2DTO(mergedAnomalyResultBean, new HashSet<>());
      return mergedAnomalyResultDTO;
    } else {
      return null;
    }
  }

  @Override
  public List<MergedAnomalyResultDTO> findByIds(List<Long> idList) {
    List<MergedAnomalyResultBean> mergedAnomalyResultBeanList =
        genericPojoDao.get(idList, MergedAnomalyResultBean.class);
    if (CollectionUtils.isNotEmpty(mergedAnomalyResultBeanList)) {
      List<MergedAnomalyResultDTO> mergedAnomalyResultDTOList =
          convertMergedAnomalyBean2DTO(mergedAnomalyResultBeanList);
      return mergedAnomalyResultDTOList;
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public List<MergedAnomalyResultDTO> findOverlappingByFunctionId(long functionId, long searchWindowStart,
      long searchWindowEnd) {
    // LT and GT are used instead of LE and GE because ThirdEye uses end time exclusive.
    Predicate predicate = Predicate
        .AND(Predicate.LT("startTime", searchWindowEnd), Predicate.GT("endTime", searchWindowStart),
            Predicate.EQ("functionId", functionId));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findOverlappingByFunctionIdDimensions(long functionId, long searchWindowStart,
      long searchWindowEnd, String dimensions) {
    // LT and GT are used instead of LE and GE because ThirdEye uses end time exclusive.
    Predicate predicate = Predicate
        .AND(Predicate.LT("startTime", searchWindowEnd), Predicate.GT("endTime", searchWindowStart),
            Predicate.EQ("functionId", functionId), Predicate.EQ("dimensions", dimensions));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByFunctionId(Long functionId) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(FIND_BY_FUNCTION_ID,
        filterParams, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByFunctionIdAndIdGreaterThan(Long functionId, Long anomalyId) {
    Predicate predicate = Predicate.AND(Predicate.EQ("functionId", functionId), Predicate.GT("baseId", anomalyId));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByDetectionConfigAndIdGreaterThan(Long detectionConfigId, Long anomalyId) {
    Predicate predicate = Predicate.AND(Predicate.EQ("detectionConfigId", detectionConfigId), Predicate.GT("baseId", anomalyId));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByStartTimeInRangeAndFunctionId(long startTime, long endTime,
      long functionId) {
    Predicate predicate =
        Predicate.AND(Predicate.LT("startTime", endTime), Predicate.GT("endTime", startTime),
            Predicate.EQ("functionId", functionId));
    return findByPredicate(predicate);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByStartEndTimeInRangeAndDetectionConfigId(long startTime, long endTime,
      long detectionConfigId) {
    Predicate predicate =
        Predicate.AND(Predicate.LT("startTime", endTime), Predicate.GT("endTime", startTime),
            Predicate.EQ("detectionConfigId", detectionConfigId));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByCreatedTimeInRangeAndDetectionConfigId(long startTime, long endTime, long detectionConfigId) {
    Predicate predicate =
        Predicate.AND(
            Predicate.GE("createTime", new Timestamp(startTime)),
            Predicate.LT("createTime", new Timestamp(endTime)),
            Predicate.EQ("detectionConfigId", detectionConfigId));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findAnomaliesWithinBoundary(long startTime, long endTime,
      long detectionConfigId) {
    Predicate predicate =
        Predicate.AND(Predicate.LE("endTime", endTime), Predicate.GE("startTime", startTime),
            Predicate.EQ("detectionConfigId", detectionConfigId));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByCollectionMetricDimensionsTime(String collection, String metric,
      String dimensions, long startTime, long endTime) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("metric", metric);
    filterParams.put("dimensions", dimensions);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME, filterParams, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection, String metric, long startTime,
      long endTime) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("metric", metric);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_COLLECTION_METRIC_TIME, filterParams, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  public List<MergedAnomalyResultDTO> findByMetricTime(String metric, long startTime, long endTime) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("metric", metric);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_METRIC_TIME, filterParams, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByCollectionTime(String collection, long startTime, long endTime) {
    Predicate predicate = Predicate
        .AND(Predicate.EQ("collection", collection), Predicate.LT("startTime", endTime),
            Predicate.GT("endTime", startTime));

    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByTime(long startTime, long endTime) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_TIME, filterParams, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  public List<MergedAnomalyResultDTO> findUnNotifiedByFunctionIdAndIdLesserThanAndEndTimeGreaterThanLastOneDay(long functionId, long anomalyId) {
    Predicate predicate = Predicate
        .AND(Predicate.EQ("functionId", functionId), Predicate.LT("baseId", anomalyId),
            Predicate.EQ("notified", false), Predicate.GT("endTime", System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1)));
    List<MergedAnomalyResultBean> list = genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return convertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findNotifiedByTime(long startTime, long endTime) {
    List<MergedAnomalyResultDTO> anomaliesByTime = findByTime(startTime, endTime);
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
      long conflictWindowStart, long conflictWindowEnd) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);
    filterParams.put("dimensions", dimensions);
    filterParams.put("startTime", conflictWindowStart);
    filterParams.put("endTime", conflictWindowEnd);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_LATEST_CONFLICT_BY_FUNCTION_AND_DIMENSIONS, filterParams, MergedAnomalyResultBean.class);

    if (CollectionUtils.isNotEmpty(list)) {
      MergedAnomalyResultBean mostRecentConflictMergedAnomalyResultBean = list.get(0);
      return convertMergedAnomalyBean2DTO(mostRecentConflictMergedAnomalyResultBean, new HashSet<>());
    }
    return null;
  }

  /**
   * Update anomaly feedback without propagation
   * @param entity Anomaly entity
   */
  @Override
  public void updateAnomalyFeedback(MergedAnomalyResultDTO entity) {
    updateAnomalyFeedback(entity, false);
  }

  /**
   * Update anomaly feedback
   * @param entity Anomaly entity
   * @param propagate Propagate the feedback to its children all the way to the leaves
   */
  @Override
  public void updateAnomalyFeedback(MergedAnomalyResultDTO entity, boolean propagate) {
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

    if (propagate) {
      for(MergedAnomalyResultDTO child: entity.getChildren()){
        child.setFeedback(feedbackDTO);
        this.updateAnomalyFeedback(child, true);
      }
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

  @Override
  public MergedAnomalyResultDTO findParent(MergedAnomalyResultDTO entity) {
    List<MergedAnomalyResultBean> candidates = genericPojoDao.get(Predicate.AND(
        Predicate.EQ("detectionConfigId", entity.getDetectionConfigId()),
        Predicate.LE("startTime", entity.getStartTime()),
        Predicate.GE("endTime", entity.getEndTime())), MergedAnomalyResultBean.class);
    for (MergedAnomalyResultBean candidate : candidates) {
      if (candidate.getChildIds() != null && !candidate.getChildIds().isEmpty()) {
        for (Long id : candidate.getChildIds()) {
          if (entity.getId().equals(id)) {
            return convertMergedAnomalyBean2DTO(candidate, new HashSet<>(Collections.singleton(candidate.getId())));
          }
        }
      }
    }
    return null;
  }

  private List<MergedAnomalyResultDTO> getAnomaliesForMetricBeanAndTimeRange(MetricConfigBean mbean, long start, long end) {

    LOG.info("Fetching anomalies for metric '{}' and dataset '{}'", mbean.getName(), mbean.getDataset());

    List<MergedAnomalyResultBean> anomalyBeans = genericPojoDao.get(
        Predicate.AND(
            Predicate.EQ("metric", mbean.getName()),
            Predicate.EQ("collection", mbean.getDataset()),
            Predicate.LT("startTime", end),
            Predicate.GT("endTime", start)
        ),
        MergedAnomalyResultBean.class);

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>(convertMergedAnomalyBean2DTO(anomalyBeans));

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

    return bean;
  }

  @Override
  public MergedAnomalyResultDTO convertMergedAnomalyBean2DTO(MergedAnomalyResultBean mergedAnomalyResultBean, Set<Long> visitedAnomalyIds) {
    MergedAnomalyResultDTO mergedAnomalyResultDTO;
    mergedAnomalyResultDTO = MODEL_MAPPER.map(mergedAnomalyResultBean, MergedAnomalyResultDTO.class);

    if (mergedAnomalyResultBean.getFunctionId() != null) {
      AnomalyFunctionBean anomalyFunctionBean = genericPojoDao.get(mergedAnomalyResultBean.getFunctionId(), AnomalyFunctionBean.class);
      AnomalyFunctionDTO anomalyFunctionDTO = MODEL_MAPPER.map(anomalyFunctionBean, AnomalyFunctionDTO.class);
      mergedAnomalyResultDTO.setFunction(anomalyFunctionDTO);
    }

    if (mergedAnomalyResultBean.getAnomalyFeedbackId() != null) {
      AnomalyFeedbackBean anomalyFeedbackBean = genericPojoDao.get(mergedAnomalyResultBean.getAnomalyFeedbackId(), AnomalyFeedbackBean.class);
      AnomalyFeedbackDTO anomalyFeedbackDTO = MODEL_MAPPER.map(anomalyFeedbackBean, AnomalyFeedbackDTO.class);
      mergedAnomalyResultDTO.setFeedback(anomalyFeedbackDTO);
    }

    visitedAnomalyIds.add(mergedAnomalyResultBean.getId());
    mergedAnomalyResultDTO.setChildren(getChildAnomalies(mergedAnomalyResultBean, visitedAnomalyIds));

    return mergedAnomalyResultDTO;
  }

  private Set<MergedAnomalyResultDTO> getChildAnomalies(MergedAnomalyResultBean mergedAnomalyResultBean, Set<Long> visitedAnomalyIds) {
    Set<MergedAnomalyResultDTO> children = new HashSet<>();
    if (mergedAnomalyResultBean.getChildIds() != null) {
      for (Long id : mergedAnomalyResultBean.getChildIds()) {
        if (id == null || visitedAnomalyIds.contains(id)) {
          continue;
        }

        MergedAnomalyResultBean childBean = genericPojoDao.get(id, MergedAnomalyResultBean.class);
        MergedAnomalyResultDTO child = convertMergedAnomalyBean2DTO(childBean, visitedAnomalyIds);
        children.add(child);
      }
    }
    return children;
  }

  @Override
  public List<MergedAnomalyResultDTO> convertMergedAnomalyBean2DTO(
      List<MergedAnomalyResultBean> mergedAnomalyResultBeanList) {
    List<Future<MergedAnomalyResultDTO>> mergedAnomalyResultDTOFutureList = new ArrayList<>(mergedAnomalyResultBeanList.size());
    for (final MergedAnomalyResultBean mergedAnomalyResultBean : mergedAnomalyResultBeanList) {
      Future<MergedAnomalyResultDTO> future =
          EXECUTOR_SERVICE.submit(new Callable<MergedAnomalyResultDTO>() {
            @Override public MergedAnomalyResultDTO call() throws Exception {
              return MergedAnomalyResultManagerImpl.this.convertMergedAnomalyBean2DTO(mergedAnomalyResultBean, new HashSet<>());
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
    return convertMergedAnomalyBean2DTO(beanList);
  }
}
