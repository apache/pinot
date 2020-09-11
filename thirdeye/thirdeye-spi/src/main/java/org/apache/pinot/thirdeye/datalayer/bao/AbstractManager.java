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
 *
 */

package org.apache.pinot.thirdeye.datalayer.bao;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
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

  /**
   * Find the entities based on the JSON value predicate
   * @param predicate the predicate
   * @return the list of entities that match with the predicate
   */
  default List<E> findByPredicateJsonVal(Predicate predicate) {
    throw new NotImplementedException("Not Implemented");
  }

  /**
   * List the entities with pagination
   * @param limit the limit for the number of elements returned
   * @param offset the offset position
   * @return the list of entities ordered by id in descending order
   */
  default List<E> list(long limit, long offset) {
    throw new NotImplementedException("Not Implemented");
  }

  /**
   * Count how many entities are there in the table
   * @return the number of total entities
   */
  default long count()  {
    throw new NotImplementedException("Not Implemented");
  }
}
