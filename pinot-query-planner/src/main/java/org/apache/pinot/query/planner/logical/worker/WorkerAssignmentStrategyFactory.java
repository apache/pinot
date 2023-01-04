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
package org.apache.pinot.query.planner.logical.worker;

import java.util.Map;
import org.apache.pinot.query.routing.WorkerManager;


public class WorkerAssignmentStrategyFactory {
  private WorkerAssignmentStrategyFactory() {
  }

  public static WorkerAssignmentStrategy create(Map<String, String> queryOptions, WorkerManager workerManager) {
    // TODO: Use queryOptions to pick a worker assignment strategy. This is not done right now since we only have 1
    //       strategy at the moment.
    return new DefaultWorkerAssignmentStrategy(workerManager);
  }
}
