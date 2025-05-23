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
package org.apache.pinot.integration.tests.logicaltable;

import java.io.File;
import java.util.List;
import java.util.Map;


public class LogicalTableWithTwelveOfflineOneRealtimeTableIntegrationTest extends BaseLogicalTableIntegrationTest {
  @Override
  protected List<String> getOfflineTableNames() {
    return List.of("o_1", "o_2", "o_3", "o_4", "o_5", "o_6", "o_7", "o_8", "o_9", "o_10", "o_11", "o_12");
  }

  @Override
  protected List<String> getRealtimeTableNames() {
    return List.of("r_1");
  }

  @Override
  protected Map<String, List<File>> getRealtimeTableDataFiles() {
    // Overlapping data files for the hybrid table
    return distributeFilesToTables(getRealtimeTableNames(),
        _avroFiles.subList(_avroFiles.size() - 2, _avroFiles.size()));
  }
}
