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
package org.apache.pinot.query.testutils;

import java.io.File;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


/// Verifies filesystem isolation between mock factories. Each test owns its factories and segment files.
public class MockInstanceDataManagerFactoryTest {
  @Test
  public void testFactoriesWithSameServerAndTableHaveIndependentDirectories()
      throws Exception {
    String serverName = "server_" + UUID.randomUUID();
    String tableName = "test_OFFLINE";
    Schema schema = new Schema.SchemaBuilder().setSchemaName("test")
        .addSingleValueDimension("id", DataType.INT)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .build();
    GenericRow row = new GenericRow();
    row.putValue("id", 1);
    row.putValue("ts", 1704067200000L);
    List<GenericRow> rows = List.of(row);
    MockInstanceDataManagerFactory firstFactory = new MockInstanceDataManagerFactory(serverName);
    firstFactory.registerTable(schema, tableName);
    ImmutableSegment firstSegment = firstFactory.addSegment(tableName, rows);
    File firstSegmentDir = firstSegment.getSegmentMetadata().getIndexDir();
    ImmutableSegment secondSegment = null;
    try {
      assertTrue(firstSegmentDir.isDirectory());
      MockInstanceDataManagerFactory secondFactory = new MockInstanceDataManagerFactory(serverName);
      secondFactory.registerTable(schema, tableName);
      assertTrue(firstSegmentDir.isDirectory(), "Registering another factory must preserve existing segment files");

      secondSegment = secondFactory.addSegment(tableName, rows);
      File secondSegmentDir = secondSegment.getSegmentMetadata().getIndexDir();
      assertTrue(secondSegmentDir.isDirectory());
      assertNotEquals(firstSegmentDir.getParentFile(), secondSegmentDir.getParentFile());
      assertTrue(firstSegmentDir.isDirectory(), "Building another factory's segment must preserve existing files");
    } finally {
      firstSegment.destroy();
      FileUtils.deleteDirectory(firstSegmentDir.getParentFile());
      if (secondSegment != null) {
        File secondSegmentDir = secondSegment.getSegmentMetadata().getIndexDir();
        secondSegment.destroy();
        FileUtils.deleteDirectory(secondSegmentDir.getParentFile());
      }
    }
  }
}
