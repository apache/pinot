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
package org.apache.pinot.core.query.distinct;

import org.apache.pinot.core.operator.blocks.ValueBlock;


/**
 * Interface class for executing the distinct queries.
 */
public interface DistinctExecutor {
  // TODO: Tune the initial capacity
  int MAX_INITIAL_CAPACITY = 10000;

  /**
   * Processes the given value block, returns {@code true} if the query is already satisfied, {@code false}
   * otherwise. No more calls should be made after it returns {@code true}.
   */
  boolean process(ValueBlock valueBlock);

  /**
   * Returns the distinct result. Note that the returned DistinctTable might not be a main DistinctTable, thus cannot be
   * used to merge other records or tables, but can only be merged into the main DistinctTable.
   */
  DistinctTable getResult();
}
