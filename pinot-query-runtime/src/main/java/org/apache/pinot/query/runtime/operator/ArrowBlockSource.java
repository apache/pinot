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
package org.apache.pinot.query.runtime.operator;


/**
 * Opt-in single-consumer edge between Arrow-aware operators. Outputs otherwise remain independent heap blocks.
 * Configure during single-threaded op-chain construction, before requesting any blocks.
 */
public interface ArrowBlockSource {
  /**
   * Allows Arrow output, transferring one owned reference to the consumer for each returned Arrow block.
   * The consumer must release it, including when discarding data or handling an error.
   */
  void enableArrowOutput();
}
