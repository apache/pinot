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
package org.apache.pinot.integration.tests.custom;

import org.testng.annotations.Test;


/// OFFLINE end-to-end ingestion test for an OPEN_STRUCT column. Builds segments from Avro via the
/// standard offline pipeline (row-major RecordReader path), uploads them, then reuses the inherited
/// index_map + dense/sparse validation against the loaded OFFLINE segment.
@Test(suiteName = "CustomClusterIntegrationTest")
public class OpenStructIngestionCommitOfflineTest extends OpenStructIngestionCommitTestBase {

  private static final String DEFAULT_TABLE_NAME = "OpenStructIngestionCommitOfflineTest";

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }
}
