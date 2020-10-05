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
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.ApplicationBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;


public class ApplicationManagerImpl extends AbstractManagerImpl<ApplicationDTO>
    implements ApplicationManager {

  @Inject
  public ApplicationManagerImpl(GenericPojoDao genericPojoDao) {
    super(ApplicationDTO.class, ApplicationBean.class, genericPojoDao);
  }

  public List<ApplicationDTO> findByName(String name) {
    Predicate predicate = Predicate.EQ("application", name);
    List<ApplicationBean> list = genericPojoDao.get(predicate, ApplicationBean.class);
    List<ApplicationDTO> result = new ArrayList<>();
    for (ApplicationBean abstractBean : list) {
      ApplicationDTO dto = MODEL_MAPPER.map(abstractBean, ApplicationDTO.class);
      result.add(dto);
    }
    return result;
  }
}
