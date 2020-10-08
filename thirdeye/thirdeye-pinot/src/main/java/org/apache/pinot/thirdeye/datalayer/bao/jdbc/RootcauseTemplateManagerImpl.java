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
import org.apache.pinot.thirdeye.datalayer.bao.RootcauseTemplateManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.RootcauseTemplateDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.RootcauseTemplateBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;

@Singleton
public class RootcauseTemplateManagerImpl extends AbstractManagerImpl<RootcauseTemplateDTO> implements
                                                                                            RootcauseTemplateManager {
  @Inject
  public RootcauseTemplateManagerImpl(GenericPojoDao genericPojoDao) {
    super(RootcauseTemplateDTO.class, RootcauseTemplateBean.class, genericPojoDao);
  }

  @Override
  public List<RootcauseTemplateDTO> findByMetricId(long metricId) {
    Predicate predicate = Predicate.EQ("metricId", metricId);
    return findByPredicate(predicate);
  }

  @Override
  public Long saveOrUpdate(RootcauseTemplateDTO rootcauseTemplateDTO) {
    Predicate predicate = Predicate.EQ("name", rootcauseTemplateDTO.getName());
    List<RootcauseTemplateDTO> list = findByPredicate(predicate);
    if (!list.isEmpty()) {
      rootcauseTemplateDTO.setId(list.get(0).getId());
      super.update(rootcauseTemplateDTO);
      return rootcauseTemplateDTO.getId();
    } else {
      return super.save(rootcauseTemplateDTO);
    }
  }
}
