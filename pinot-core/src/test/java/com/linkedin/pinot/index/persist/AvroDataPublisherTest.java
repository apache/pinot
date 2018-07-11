/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.index.persist;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.util.AvroUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroDataPublisherTest {

  private final String AVRO_DATA = "data/test_sample_data.avro";
  private final String JSON_DATA = "data/test_sample_data.json";
  private final String AVRO_MULTI_DATA = "data/test_sample_data_multi_value.avro";

  @Test
  public void TestReadAvro() throws Exception {

    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));
    final String jsonPath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(JSON_DATA));

    Schema schema =
        new Schema.SchemaBuilder().addSingleValueDimension("column3", DataType.STRING)
            .addSingleValueDimension("column2", DataType.STRING).build();

    final SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setFormat(FileFormat.AVRO);
    config.setInputFilePath(filePath);

    config.setSegmentVersion(SegmentVersion.v1);

    AvroRecordReader avroDataPublisher = (AvroRecordReader) RecordReaderFactory.getRecordReader(config);

    int cnt = 0;
    for (String line : FileUtils.readLines(new File(jsonPath))) {

      JSONObject obj = new JSONObject(line);
      if (avroDataPublisher.hasNext()) {
        GenericRow recordRow = avroDataPublisher.next();

        for (String column : recordRow.getFieldNames()) {
          String valueFromJson = obj.get(column).toString();
          String valueFromAvro = recordRow.getValue(column).toString();
          if (cnt > 1) {
            Assert.assertEquals(valueFromJson, valueFromAvro);
          }
        }
      }
      cnt++;
    }
    Assert.assertEquals(cnt, 10001);
  }

  @Test
  public void TestReadPartialAvro() throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));
    final String jsonPath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(JSON_DATA));

    final List<String> projectedColumns = new ArrayList<String>();
    projectedColumns.add("column3");
    projectedColumns.add("column2");

    Schema schema =
        new Schema.SchemaBuilder().addSingleValueDimension("column3", DataType.STRING)
            .addSingleValueDimension("column2", DataType.STRING).build();
    final SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);

    config.setFormat(FileFormat.AVRO);
    config.setInputFilePath(filePath);

    config.setSegmentVersion(SegmentVersion.v1);

    AvroRecordReader avroDataPublisher = (AvroRecordReader) RecordReaderFactory.getRecordReader(config);

    int cnt = 0;
    for (final String line : FileUtils.readLines(new File(jsonPath))) {

      final JSONObject obj = new JSONObject(line);
      if (avroDataPublisher.hasNext()) {
        final GenericRow recordRow = avroDataPublisher.next();
        // System.out.println(recordRow);
        Assert.assertEquals(recordRow.getFieldNames().length, 2);
        for (final String column : recordRow.getFieldNames()) {
          final String valueFromJson = obj.get(column).toString();
          final String valueFromAvro = recordRow.getValue(column).toString();
          if (cnt > 1) {
            Assert.assertEquals(valueFromAvro, valueFromJson);
          }
        }
      }
      cnt++;
    }
    Assert.assertEquals(10001, cnt);
  }

  @Test
  public void TestReadMultiValueAvro() throws Exception {

    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_MULTI_DATA));

    final SegmentGeneratorConfig config =
        new SegmentGeneratorConfig(AvroUtils.getPinotSchemaFromAvroDataFile(new File(filePath)));
    config.setFormat(FileFormat.AVRO);
    config.setInputFilePath(filePath);

    config.setSegmentVersion(SegmentVersion.v1);

    AvroRecordReader avroDataPublisher = (AvroRecordReader) RecordReaderFactory.getRecordReader(config);

    int cnt = 0;

    while (avroDataPublisher.hasNext()) {
      GenericRow recordRow = avroDataPublisher.next();
      for (String column : recordRow.getFieldNames()) {
        String valueStringFromAvro = null;
        if (avroDataPublisher.getSchema().getFieldSpecFor(column).isSingleValueField()) {
          Object valueFromAvro = recordRow.getValue(column);
          valueStringFromAvro = valueFromAvro.toString();
        } else {
          Object[] valueFromAvro = (Object[]) recordRow.getValue(column);
          valueStringFromAvro = "[";
          int i = 0;
          for (Object valueObject : valueFromAvro) {
            if (i++ == 0) {
              valueStringFromAvro += valueObject.toString();
            } else {
              valueStringFromAvro += ", " + valueObject.toString();
            }
          }
          valueStringFromAvro += "]";
        }

      }
      cnt++;
    }
    Assert.assertEquals(28949, cnt);
  }
}
