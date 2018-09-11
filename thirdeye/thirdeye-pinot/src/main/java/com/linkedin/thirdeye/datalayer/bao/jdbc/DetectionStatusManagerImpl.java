/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
}
