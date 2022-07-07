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
package org.apache.pinot.query.planner.partitioning;


/**
 * The {@code KeySelector} provides a partitioning function to encode a specific input data type into a key.
 *
 * <p>This key selector is used for computation such as GROUP BY or equality JOINs.
 *
 * <p>Key selector should always produce the same selection hash key when the same input is provided.
 */
public interface KeySelector<IN, OUT> {

  /**
   * Extract the key out of an input data construct.
   *
   * @param input input data.
   * @return the key of the input data.
   */
  OUT getKey(IN input);

  int computeHash(IN input);
}
