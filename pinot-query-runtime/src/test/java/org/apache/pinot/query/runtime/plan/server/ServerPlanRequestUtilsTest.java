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
package org.apache.pinot.query.runtime.plan.server;

import java.util.List;
import java.util.Map;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Verifies request ID allocation for leaf workers sharing a server.
public class ServerPlanRequestUtilsTest {
  @Test
  public void testLeafRequestIdsPreserveTableTypeAndSeparateLocalWorkers() {
    var workers = List.of(new WorkerMetadata(11, Map.of()), new WorkerMetadata(37, Map.of()));
    long offline = 4321L << 16;
    for (long base : List.of(offline, offline + 1)) {
      assertEquals(ServerPlanRequestUtils.localLeafRequestId(base, 11, workers), base);
      assertEquals(ServerPlanRequestUtils.localLeafRequestId(base, 37, workers), base + 2);
      assertEquals(ServerPlanRequestUtils.localLeafRequestId(base, 37, List.of(workers.get(1))), base);
    }
  }
}
