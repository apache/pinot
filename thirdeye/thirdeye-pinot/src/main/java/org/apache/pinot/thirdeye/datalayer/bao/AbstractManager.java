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

package org.apache.pinot.thirdeye.datalayer.bao;

import java.util.List;
import java.util.Map;

import org.apache.pinot.thirdeye.datalayer.dto.AbstractDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;


public interface AbstractManager<E extends AbstractDTO> {

  Long save(E entity);

  int update(E entity);

  int update(List<E> entities);

  E findById(Long id);

  List<E> findByIds(List<Long> id);

  int delete(E entity);

  int deleteById(Long id);

  int deleteByIds(List<Long> ids);

  int deleteByPredicate(Predicate predicate);

  int deleteRecordsOlderThanDays(int days);

  List<E> findAll();

  List<E> findByParams(Map<String, Object> filters);

  List<E> findByPredicate(Predicate predicate);

  List<Long> findIdsByPredicate(Predicate predicate);

  int update(E entity, Predicate predicate);
}
