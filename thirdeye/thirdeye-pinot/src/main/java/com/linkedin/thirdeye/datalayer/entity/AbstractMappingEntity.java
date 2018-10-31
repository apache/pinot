/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datalayer.entity;

import java.util.List;

public abstract class AbstractMappingEntity<P extends AbstractJsonEntity, Q extends AbstractJsonEntity> {
  List<P> ps;
  List<Q> qs;

  protected AbstractMappingEntity(List<Long> pIds, List<Long> qIds) {
    // TODO : populate mapped entities, Table and column names can be derived from Class name,
    // and P/Q class types

    // query : select pColumnName, qColumnName from mappingTable where pColumnName in :pIds
    // qColumnName in :qIds
    // Then fetch the entities and set in the list
  }
}
