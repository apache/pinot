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
import org.apache.pinot.thirdeye.datalayer.bao.ClassificationConfigManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.ClassificationConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.ClassificationConfigBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;

@Singleton
public class ClassificationConfigManagerImpl extends AbstractManagerImpl<ClassificationConfigDTO>
    implements ClassificationConfigManager {

  @Inject
  public ClassificationConfigManagerImpl(GenericPojoDao genericPojoDao) {
    super(ClassificationConfigDTO.class, ClassificationConfigBean.class, genericPojoDao);
  }

  @Override
  public List<ClassificationConfigDTO> findActives() {
    Predicate predicate = Predicate.EQ("active", true);
    return findByPredicate(predicate);
  }

  @Override
  public ClassificationConfigDTO findByName(String name) {
    Predicate predicate = Predicate.EQ("name", name);
    List<ClassificationConfigDTO> results = findByPredicate(predicate);

    if (CollectionUtils.isNotEmpty(results)) {
      return results.get(0);
    } else {
      return null;
    }
  }
}
