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
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.datalayer.bao.RootcauseSessionManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.RootcauseSessionDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AbstractBean;
import org.apache.pinot.thirdeye.datalayer.pojo.RootcauseSessionBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;


@Singleton
public class RootcauseSessionManagerImpl extends AbstractManagerImpl<RootcauseSessionDTO> implements RootcauseSessionManager {
  private static final String FIND_BY_LIKE_TEMPLATE = "WHERE %s";
  private static final String FIND_BY_LIKE_JOINER = " AND ";
  private static final String FIND_BY_LIKE_VALUE = "%%%s%%";

  private static final String FIND_BY_NAME_LIKE_TEMPLATE = "name LIKE :name__%d";
  private static final String FIND_BY_NAME_LIKE_KEY = "name__%d";

  @Inject
  public RootcauseSessionManagerImpl(GenericPojoDao genericPojoDao) {
    super(RootcauseSessionDTO.class, RootcauseSessionBean.class, genericPojoDao);
  }

  @Override
  public List<RootcauseSessionDTO> findByName(String name) {
    return findByPredicate(Predicate.EQ("name", name));
  }

  @Override
  public List<RootcauseSessionDTO> findByNameLike(Set<String> nameFragments) {
    return findByLike(nameFragments, FIND_BY_NAME_LIKE_TEMPLATE, FIND_BY_NAME_LIKE_KEY);
  }

  @Override
  public List<RootcauseSessionDTO> findByOwner(String owner) {
    return findByPredicate(Predicate.EQ("owner", owner));
  }

  @Override
  public List<RootcauseSessionDTO> findByAnomalyRange(long start, long end) {
    return findByPredicate(Predicate.AND(Predicate.GT("anomalyRangeEnd", start), Predicate.LT("anomalyRangeStart", end)));
  }

  @Override
  public List<RootcauseSessionDTO> findByCreatedRange(long start, long end) {
    return findByPredicate(Predicate.AND(Predicate.GE("created", start), Predicate.LT("created", end)));
  }

  @Override
  public List<RootcauseSessionDTO> findByUpdatedRange(long start, long end) {
    return findByPredicate(Predicate.AND(Predicate.GE("updated", start), Predicate.LT("updated", end)));
  }

  @Override
  public List<RootcauseSessionDTO> findByPreviousId(long id) {
    return findByPredicate(Predicate.EQ("previousId", id));
  }

  @Override
  public List<RootcauseSessionDTO> findByAnomalyId(long id) {
    return findByPredicate(Predicate.EQ("anomalyId", id));
  }

  private List<RootcauseSessionDTO> findByLike(Set<String> fragments, String template, String key) {
    return findByLike(fragments, template, key, RootcauseSessionDTO.class, RootcauseSessionBean.class);
  }

  private <B extends AbstractBean, D> List<D> findByLike(Set<String> fragments, String template, String key,
      Class<D> dtoClass, Class<B> beanClass) {
    List<String> conditions = new ArrayList<>();
    Map<String, Object> params = new HashMap<>();

    int i = 0;
    for (String fragment : fragments) {
      conditions.add(String.format(template, i));
      params.put(String.format(key, i), String.format(FIND_BY_LIKE_VALUE, fragment));
      i++;
    }

    String query = String.format(FIND_BY_LIKE_TEMPLATE, StringUtils.join(conditions, FIND_BY_LIKE_JOINER));

    List<B> beans = genericPojoDao.executeParameterizedSQL(query, params, beanClass);

    List<D> dtos = new ArrayList<>();
    for (B bean : beans) {
      dtos.add(MODEL_MAPPER.map(bean, dtoClass));
    }

    return dtos;
  }
}
