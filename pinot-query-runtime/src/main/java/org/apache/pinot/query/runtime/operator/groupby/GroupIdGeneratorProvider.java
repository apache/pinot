/**
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
package org.apache.pinot.query.runtime.operator.groupby;

import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/// Creates one group-ID generator for a multi-stage aggregate operator. Non-default providers are invoked lazily
/// when the operator first executes, never during plan construction. Implementations own a returned generator until
/// the caller receives it and must close any resources they allocate if construction fails. Values returned by the
/// generator's key iterator must remain valid after the generator closes; native implementations must detach them.
/// The operator closes a successfully returned generator at most once, so implementations also own any cleanup
/// failure handling and accounting.
@FunctionalInterface
public interface GroupIdGeneratorProvider {
  GroupIdGeneratorProvider DEFAULT = GroupIdGeneratorFactory::getGroupIdGenerator;

  /// The first `numKeyColumns` entries of `storedTypes` describe the key columns. The limits are already resolved
  /// from the operator's query options and hints, so a provider must not reinterpret their precedence.
  GroupIdGenerator create(ColumnDataType[] storedTypes, int numKeyColumns, int numGroupsLimit,
      int maxInitialResultHolderCapacity);
}
