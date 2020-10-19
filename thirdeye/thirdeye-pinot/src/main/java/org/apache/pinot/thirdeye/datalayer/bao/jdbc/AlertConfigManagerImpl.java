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
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;

@Singleton
public class AlertConfigManagerImpl extends AbstractManagerImpl<AlertConfigDTO>
    implements AlertConfigManager {
  private static final String FIND_BY_NAME_LIKE = " WHERE name like :name";
  private static final String FIND_BY_NAME_EQUALS = " WHERE name = :name";
  private static final String FIND_BY_APPLICATION_LIKE = " WHERE application like :application";

  @Inject
  public AlertConfigManagerImpl(GenericPojoDao genericPojoDao) {
    super(AlertConfigDTO.class, AlertConfigBean.class, genericPojoDao);
  }

  @Override
  public List<AlertConfigDTO> findByActive(boolean active) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("active", active);
    return super.findByParams(filters);
  }

  @Override
  public List<AlertConfigDTO> findWhereNameLike(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("name", name);
    List<AlertConfigBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_LIKE, parameterMap, AlertConfigBean.class);
    List<AlertConfigDTO> result = new ArrayList<>();
    for (AlertConfigBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, AlertConfigDTO.class));
    }
    return result;
  }

  @Override
  public AlertConfigDTO findWhereNameEquals(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("name", name);
    List<AlertConfigBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_EQUALS, parameterMap, AlertConfigBean.class);
    List<AlertConfigDTO> result = new ArrayList<>();
    for (AlertConfigBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, AlertConfigDTO.class));
    }
    return result.isEmpty()? null : result.get(0);
  }

  @Override
  public List<AlertConfigDTO> findWhereApplicationLike(String application) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("application", application);
    List<AlertConfigBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_APPLICATION_LIKE, parameterMap, AlertConfigBean.class);
    List<AlertConfigDTO> result = new ArrayList<>();
    for (AlertConfigBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, AlertConfigDTO.class));
    }
    return result;
  }

  @Override
  public List<AlertConfigDTO> findByFunctionId(Long functionId) {
    List<AlertConfigBean> list = genericPojoDao.getAll(AlertConfigBean.class);
    List<AlertConfigDTO> result = new ArrayList<>();
    for (AlertConfigBean bean : list) {
      if (bean.getEmailConfig() != null && bean.getEmailConfig().getFunctionIds().contains(functionId)) {
        result.add(MODEL_MAPPER.map(bean, AlertConfigDTO.class));
      }
    }
    return result;
  }

}
