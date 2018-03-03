package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.bao.DetectionStatusManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.datalayer.pojo.DetectionStatusBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

@Singleton
public class DetectionStatusManagerImpl extends AbstractManagerImpl<DetectionStatusDTO>
    implements DetectionStatusManager {

  public DetectionStatusManagerImpl() {
    super(DetectionStatusDTO.class, DetectionStatusBean.class);
  }

  @Override
  public DetectionStatusDTO findLatestEntryForFunctionId(long functionId) {
    Predicate predicate = Predicate.EQ("functionId", functionId);
    List<DetectionStatusBean> list = genericPojoDao.get(predicate, DetectionStatusBean.class);
    DetectionStatusDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      Collections.sort(list);
      result = MODEL_MAPPER.map(list.get(list.size() - 1), DetectionStatusDTO.class);
    }
    return result;
  }

  @Override
  public List<DetectionStatusDTO> findAllInTimeRangeForFunctionAndDetectionRun(long startTime, long endTime,
      long functionId, boolean detectionRun) {
    Predicate predicate = Predicate.AND(
        Predicate.EQ("functionId", functionId),
        Predicate.LE("dateToCheckInMS", endTime),
        Predicate.GE("dateToCheckInMS", startTime),
        Predicate.EQ("detectionRun", detectionRun));

    return findByPredicate(predicate);
  }

  @Override
  @Transactional
  public int deleteRecordsOlderThanDays(int days) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    Predicate timestampPredicate = Predicate.LT("createTime", expireTimestamp);
    List<DetectionStatusBean> list = genericPojoDao.get(timestampPredicate, DetectionStatusBean.class);
    for (DetectionStatusBean bean : list) {
      deleteById(bean.getId());
    }
    return list.size();
  }
}
