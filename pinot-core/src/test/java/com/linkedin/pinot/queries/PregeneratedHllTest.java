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

package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.query.gen.AvroQueryGenerator;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.startree.hll.HllConfig;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import com.linkedin.pinot.core.startree.hll.SegmentWithHllIndexCreateHelper;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PregeneratedHllTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PregeneratedHllTest.class);
  private static final HllConfig hllConfig = new HllConfig(12 /*log2m*/);
  private TestHelper testHelper;
  private SegmentWithHllIndexCreateHelper helper;
  private static final double approximationThreshold = 0.001;

  @BeforeClass
  public void setup()
      throws Exception {
    testHelper = new TestHelper("testTable", null);
    testHelper.init();
  }

  @AfterClass
  public void tearDown() {
    helper.cleanTempDir();
    testHelper.close();
  }

  public File createAvroWithHll(File newAvroFile, String inputAvro, String column, int log2m)
      throws IOException {
    String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(inputAvro));
    try( DataFileStream<GenericRecord> avroReader = AvroUtils.getAvroReader(new File(filePath))) {
      Schema currentSchema = avroReader.getSchema();
      List<Schema.Field> fields = currentSchema.getFields();
      List<Schema.Field> newFieldList = new ArrayList<>(fields.size());
      for (Schema.Field field : fields) {
        newFieldList.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()));
      }
      final String hllColumnName = column + "_hll";
      newFieldList.add(new Schema.Field(hllColumnName, Schema.create(Schema.Type.STRING), null, null));
      Schema updatedSchema = Schema.createRecord("hllschema", "doc", this.getClass().getName(), false);
      updatedSchema.setFields(newFieldList);
      try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<GenericData.Record>(new GenericDatumWriter<GenericData.Record>(updatedSchema))) {
        writer.create(updatedSchema, newAvroFile);
        while (avroReader.hasNext()) {
          GenericRecord record = avroReader.next();
          GenericData.Record newRecord = new GenericData.Record(updatedSchema);
          for (Schema.Field field : fields) {
            newRecord.put(field.name(), record.get(field.name()));
          }
          newRecord.put(hllColumnName, HllUtil.singleValueHllAsString(log2m, record.get(column)));
          writer.append(newRecord);
        }
      }
    }
    return newAvroFile;
  }

  private void createLoadSegment(String segmentName, File destinationDir)
      throws Exception {
    final String column = "column1";
    File newAvroFile = new File(destinationDir, segmentName + ".avro");
    createAvroWithHll(newAvroFile, testHelper.AVRO_DATA, column, hllConfig.getHllLog2m());
    helper = testHelper.buildHllSegment(testHelper.tableName, newAvroFile.getAbsolutePath(), testHelper.TIME_COLUMN,
        testHelper.TIME_UNIT, segmentName, hllConfig, false);
    testHelper.loadSegment(helper.getSegmentDirectory());
  }

  @Test
  public void testHllOnPregeneratedColumn()
      throws Exception {
    File destinationDir = null;
    try {
      destinationDir = Files.createTempDirectory(PregeneratedHllTest.class.getName()).toFile();

      final String segmentName = "pregeneratedHllSegment";
      createLoadSegment(segmentName, destinationDir);

      // Init Query Executor
      final List<AvroQueryGenerator.TestSimpleAggreationQuery> aggCalls = new ArrayList<>();
      aggCalls.add(
          new AvroQueryGenerator.TestSimpleAggreationQuery("select fasthll(column1_hll) from " + testHelper.tableName +
              " where column1 > 100000 limit 0", 0.0));
      aggCalls.add(new AvroQueryGenerator.TestSimpleAggreationQuery(
          "select distinctcounthll(column1) from " + testHelper.tableName +
              " where column1 > 100000 limit 0", 0.0));

      // we give high value of 1000 because this test also runs with log2m different from distinctCounthll
      // function. There is no guarantee about counts or accuracy. I want to make sure these are valid values
      ApproximateQueryTestUtil
          .runApproximationQueries(testHelper.queryExecutor, helper.getSegmentName(), aggCalls, 1000,
              testHelper.serverMetrics);

      Double val1 = (Double)ApproximateQueryTestUtil
          .runQuery(testHelper.queryExecutor, Arrays.asList(helper.getSegmentName()), aggCalls.get(0),
              testHelper.serverMetrics);
      // 5000, because we know
      Assert.assertTrue(val1 > 5000);
      // add one more segment. Goal to verify that combine works correctly
      // across multiple segments. Not interested in exact values
      final String secondSegmentName = "segment2";
      createLoadSegment(secondSegmentName, destinationDir);
      ApproximateQueryTestUtil
          .runApproximationQueries(testHelper.queryExecutor, Arrays.asList(helper.getSegmentName(), secondSegmentName),
              aggCalls, 1000, testHelper.serverMetrics);
      Double val2 = (Double)ApproximateQueryTestUtil
          .runQuery(testHelper.queryExecutor, Arrays.asList(helper.getSegmentName(), secondSegmentName), aggCalls.get(0),
              testHelper.serverMetrics);
      // both segments are same....distinct count should be same
      Assert.assertEquals(val1, val2);

    } finally {
      FileUtils.deleteQuietly(destinationDir);
    }
  }
}
