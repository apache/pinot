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
package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Optional;


/**
 * This interfaces carries the RLS/CLS filter for a particular table
 */
public interface TableRowColAccessResult {
  /**
   * Returns the RLS filters associated with a particular table. RLS filters are defined as a list.
   * @return optional of the RLS filters. Empty optional if there are no RLS filters defined on this table
   */
  Optional<List<String>> getRLSFilters();

  void setRLSFilters(List<String> rlsFilters);
}
