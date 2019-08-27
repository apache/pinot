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
package org.apache.pinot.controller.helix.core.assignment.instance;

import java.util.List;
import java.util.Map;
import org.apache.helix.model.InstanceConfig;


/**
 * The instance constraint applier is responsible for filtering out unqualified instances and sorts instances for
 * picking priority.
 */
public interface InstanceConstraintApplier {

  /**
   * Applies the instance constraint to the given map from pool to instance configs to filter out unqualified instances
   * and sort instances for picking priority.
   */
  Map<Integer, List<InstanceConfig>> applyConstraint(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap);
}
