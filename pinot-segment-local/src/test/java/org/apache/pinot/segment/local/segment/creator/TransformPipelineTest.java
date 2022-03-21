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
package org.apache.pinot.segment.local.segment.creator;

import java.util.Collection;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;



public class TransformPipelineTest {

  private static TableConfig createTestTableConfig()
      throws Exception {
    return Fixtures.createTableConfig("some.consumer.class", "some.decoder.class");
  }

  @Test
  public void testSingleRow()
      throws Exception {
    TableConfig config = createTestTableConfig();
    Schema schema = Fixtures.createSchema();
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow simpleRow = Fixtures.createSingleRow(9527);
    TransformPipeline.Result result = new TransformPipeline.Result();
    pipeline.processRow(simpleRow, result);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getTransformedRows().size(), 1);
    Assert.assertEquals(result.getSkippedRowCount(), 0);
    Assert.assertEquals(result.getTransformedRows().get(0), simpleRow);
  }

  @Test
  public void testSingleRowFailure()
      throws Exception {
    TableConfig config = createTestTableConfig();
    Schema schema = Fixtures.createSchema();
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow simpleRow = Fixtures.createInvalidSingleRow(9527);
    boolean exceptionThrown = false;
    TransformPipeline.Result result = new TransformPipeline.Result();
    try {
      pipeline.processRow(simpleRow, result);
    } catch (Exception ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getTransformedRows().size(), 0);
    Assert.assertEquals(result.getSkippedRowCount(), 0);
  }

  @Test
  public void testMultipleRow()
      throws Exception {
    TableConfig config = createTestTableConfig();
    Schema schema = Fixtures.createSchema();
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow multipleRow = Fixtures.createMultipleRow(9527);
    Collection<GenericRow> rows = (Collection<GenericRow>) multipleRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    TransformPipeline.Result result = new TransformPipeline.Result();
    pipeline.processRow(multipleRow, result);

    Assert.assertNotNull(result);
    Assert.assertEquals(result.getTransformedRows().size(), rows.size());
    Assert.assertEquals(result.getSkippedRowCount(), 0);
    Assert.assertEquals(result.getTransformedRows(), rows);
  }

  @Test
  public void testMultipleRowPartialFailure()
      throws Exception {
    TableConfig config = createTestTableConfig();
    Schema schema = Fixtures.createSchema();
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow multipleRow = Fixtures.createMultipleRowPartialFailure(9527);
    TransformPipeline.Result result = new TransformPipeline.Result();
    boolean exceptionThrown = false;
    try {
      pipeline.processRow(multipleRow, result);
    } catch (Exception ex) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getTransformedRows().size(), 1);
    Assert.assertEquals(result.getSkippedRowCount(), 0);
  }

  @Test
  public void testReuseResultSet()
      throws Exception {
    TableConfig config = createTestTableConfig();
    Schema schema = Fixtures.createSchema();
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow simpleRow = Fixtures.createSingleRow(9527);

    TransformPipeline.Result result = new TransformPipeline.Result();
    pipeline.processRow(simpleRow, result);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getTransformedRows().size(), 1);
    Assert.assertEquals(result.getSkippedRowCount(), 0);
    Assert.assertEquals(result.getTransformedRows().get(0), simpleRow);

    // same row runs twice, should reset the flag.
    pipeline.processRow(simpleRow, result);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getTransformedRows().size(), 1);
    Assert.assertEquals(result.getSkippedRowCount(), 0);
    Assert.assertEquals(result.getTransformedRows().get(0), simpleRow);
  }
}
