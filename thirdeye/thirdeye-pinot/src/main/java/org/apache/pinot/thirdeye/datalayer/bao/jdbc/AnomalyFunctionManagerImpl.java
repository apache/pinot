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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;


@Singleton
public class AnomalyFunctionManagerImpl extends AbstractManagerImpl<AnomalyFunctionDTO>
    implements AnomalyFunctionManager {
  private static final String FIND_BY_NAME_LIKE = " WHERE functionName like :functionName";
  private static final String FIND_BY_NAME_EQUALS = " WHERE functionName = :functionName";

  @Inject
  public AnomalyFunctionManagerImpl(GenericPojoDao genericPojoDao) {
    super(AnomalyFunctionDTO.class, AnomalyFunctionBean.class, genericPojoDao);
  }

  @Override
  public List<AnomalyFunctionDTO> findAllByCollection(String collection) {
    Predicate predicate = Predicate.EQ("collection", collection);
    List<AnomalyFunctionBean> list = genericPojoDao.get(predicate, AnomalyFunctionBean.class);
    List<AnomalyFunctionDTO> result = new ArrayList<>();
    for (AnomalyFunctionBean abstractBean : list) {
      AnomalyFunctionDTO dto = MODEL_MAPPER.map(abstractBean, AnomalyFunctionDTO.class);
      result.add(dto);
    }
    return result;
  }

  /**
   * Get the list of anomaly functions under the given application
   * @param application name of the application
   * @return return the list of anomaly functions under the application
   */
  @Override
  public List<AnomalyFunctionDTO> findAllByApplication(String application) {
    if (StringUtils.isBlank(application)) {
      throw new IllegalArgumentException("application is null or empty");
    }

    // get the list of function ids from the alert config under the application
    Set<Long> applicationFunctionIds = new HashSet<>();
    List<AlertConfigBean> alerts =
        genericPojoDao.get(Predicate.EQ("application", application), AlertConfigBean.class);
    for (AlertConfigBean alert : alerts) {
      applicationFunctionIds.addAll(alert.getEmailConfig().getFunctionIds());
    }

    // Get the anomaly function dto from the function id fetched ahead
    List<AnomalyFunctionBean> applicationAnomalyFunctionBeans = genericPojoDao
        .get(new ArrayList<Long>(applicationFunctionIds), AnomalyFunctionBean.class);
    List<AnomalyFunctionDTO> applicationAnomalyFunctions = new ArrayList<>();
    for (AnomalyFunctionBean abstractBean : applicationAnomalyFunctionBeans) {
      AnomalyFunctionDTO dto = MODEL_MAPPER.map(abstractBean, AnomalyFunctionDTO.class);
      applicationAnomalyFunctions.add(dto);
    }
    return applicationAnomalyFunctions;
  }

  @Override
  public List<String> findDistinctTopicMetricsByCollection(String collection) {
    Predicate predicate = Predicate.EQ("collection", collection);
    List<AnomalyFunctionDTO> dtoList = findByPredicate(predicate);
    Set<String> metrics = new HashSet<>();
    for (AnomalyFunctionDTO dto : dtoList) {
      metrics.add(dto.getTopicMetric());
    }
    return new ArrayList<>(metrics);
  }

  @Override
  public List<AnomalyFunctionDTO> findAllActiveFunctions() {
    Predicate predicate = Predicate.EQ("active", true);
    return findByPredicate(predicate);
  }

  @Override
  public List<AnomalyFunctionDTO> findWhereNameLike(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("functionName", name);
    List<AnomalyFunctionBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_LIKE, parameterMap, AnomalyFunctionBean.class);
    List<AnomalyFunctionDTO> result = new ArrayList<>();
    for (AnomalyFunctionBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, AnomalyFunctionDTO.class));
    }
    return result;
  }

  @Override
  public AnomalyFunctionDTO findWhereNameEquals(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("functionName", name);
    List<AnomalyFunctionBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_EQUALS, parameterMap, AnomalyFunctionBean.class);
    List<AnomalyFunctionDTO> result = new ArrayList<>();
    for (AnomalyFunctionBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, AnomalyFunctionDTO.class));
    }
    return result.isEmpty()? null : result.get(0);
  }
}
