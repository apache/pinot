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
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.EntityToEntityMappingBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;

@Singleton
public class EntityToEntityMappingManagerImpl extends AbstractManagerImpl<EntityToEntityMappingDTO> implements EntityToEntityMappingManager {
  @Inject
  public EntityToEntityMappingManagerImpl(GenericPojoDao genericPojoDao) {
    super(EntityToEntityMappingDTO.class, EntityToEntityMappingBean.class, genericPojoDao);
  }

  @Override
  public List<EntityToEntityMappingDTO> findByFromURN(String fromURN) {
    return findByPredicate(Predicate.EQ("fromURN", fromURN));
  }

  @Override
  public List<EntityToEntityMappingDTO> findByFromURNs(Set<String> fromURNs) {
    return findByPredicate(Predicate.IN("fromURN", fromURNs.toArray()));
  }

  @Override
  public List<EntityToEntityMappingDTO> findByToURN(String toURN) {
    return findByPredicate(Predicate.EQ("toURN", toURN));
  }

  @Override
  public List<EntityToEntityMappingDTO> findByToURNs(Set<String> toURNs) {
    return findByPredicate(Predicate.IN("toURN", toURNs.toArray()));
  }

  @Override
  public EntityToEntityMappingDTO findByFromAndToURN(String fromURN, String toURN) {
    EntityToEntityMappingDTO dto = null;
    Predicate predicate = Predicate.AND(Predicate.EQ("fromURN", fromURN), Predicate.EQ("toURN", toURN));
    List<EntityToEntityMappingDTO> findByPredicate = findByPredicate(predicate);
    if (CollectionUtils.isNotEmpty(findByPredicate)) {
      dto = findByPredicate.get(0);
    }
    return dto;
  }

  @Override
  public List<EntityToEntityMappingDTO> findByMappingType(String mappingType) {
    return findByPredicate(Predicate.EQ("mappingType", mappingType));
  }

  @Override
  public List<EntityToEntityMappingDTO> findByFromURNAndMappingType(String fromURN, String mappingType) {
    return findByPredicate(Predicate.AND(Predicate.EQ("fromURN", fromURN), Predicate.EQ("mappingType", mappingType)));
  }

  @Override
  public List<EntityToEntityMappingDTO> findByToURNAndMappingType(String toURN, String mappingType) {
    return findByPredicate(Predicate.AND(Predicate.EQ("toURN", toURN), Predicate.EQ("mappingType", mappingType)));
  }
}
