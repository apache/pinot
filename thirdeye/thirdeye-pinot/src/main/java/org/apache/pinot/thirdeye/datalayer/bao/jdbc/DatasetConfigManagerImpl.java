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
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.DatasetConfigBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;

@Singleton
public class DatasetConfigManagerImpl extends AbstractManagerImpl<DatasetConfigDTO>
    implements DatasetConfigManager {

  @Inject
  public DatasetConfigManagerImpl(GenericPojoDao genericPojoDao) {
    super(DatasetConfigDTO.class, DatasetConfigBean.class, genericPojoDao);
  }


  @Override
  public DatasetConfigDTO findByDataset(String dataset) {
    Predicate predicate = Predicate.EQ("dataset", dataset);
    List<DatasetConfigBean> list = genericPojoDao.get(predicate, DatasetConfigBean.class);
    DatasetConfigDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      result = MODEL_MAPPER.map(list.get(0), DatasetConfigDTO.class);
    }
    return result;
  }

  @Override
  public List<DatasetConfigDTO> findActive() {
    Predicate activePredicate = Predicate.EQ("active", true);
    return findByPredicate(activePredicate);
  }

  @Override
  public void updateLastRefreshTime(String dataset, long refreshTime, long eventTime) {
    DatasetConfigDTO datasetConfigDTO = findByDataset(dataset);
    datasetConfigDTO.setLastRefreshTime(refreshTime);
    datasetConfigDTO.setLastRefreshEventTime(eventTime);
    update(datasetConfigDTO);
  }
}
