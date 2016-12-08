/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.exception.QueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

// A test to make sure we have serialization and deserialization of DataTable working correctly across versions.
// TODO Add other test cases here.
public class DataTableTest {
  public static Logger LOGGER = LoggerFactory.getLogger(DataTable.class);

  // Test Datatable with just metadata
  @Test
  public void testSerde1() throws Exception {
    DataTable dataTable = new DataTable();
    Exception e = new RuntimeException("Testing query execution exception");
    dataTable.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
    final long execTimeMs = 243;
    final long requestId = 0xfeedbeef;
    dataTable.getMetadata().put(DataTable.TIME_USED_MS_METADATA_KEY, Long.toString((execTimeMs)));
    dataTable.getMetadata().put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(requestId));
    dataTable.getMetadata().put(DataTable.TRACE_INFO_METADATA_KEY, "false");

    byte[] dataTableBytes = dataTable.toBytes();

    DataTable receivedTable = new DataTable(dataTableBytes);
    LOGGER.trace(receivedTable.toString());
  }
}
