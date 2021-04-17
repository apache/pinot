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
package org.apache.pinot.hadoop.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class PinotOutputFormatTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "PinotOutputFormatTest");
  private static final String RAW_TABLE_NAME = "testTable";

  @BeforeClass
  public void setUp() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileUtils.forceDelete(TEMP_DIR);
  }

  @Test
  public void testPinotOutputFormat() throws Exception {
    Job job = Job.getInstance();
    File outputDir = new File(TEMP_DIR, "output");
    File tempSegmentDir = new File(TEMP_DIR, "tempSegment");
    PinotOutputFormat.setOutputPath(job, new Path(outputDir.getAbsolutePath()));
    PinotOutputFormat.setTempSegmentDir(job, tempSegmentDir.getAbsolutePath());
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    PinotOutputFormat.setTableConfig(job, tableConfig);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING).addMetric("salary", FieldSpec.DataType.INT).build();
    PinotOutputFormat.setSchema(job, schema);
    PinotOutputFormat.setFieldExtractorClass(job, JsonBasedFieldExtractor.class);

    TaskAttemptContext taskAttemptContext = mock(TaskAttemptContext.class);
    when(taskAttemptContext.getConfiguration()).thenReturn(job.getConfiguration());
    PinotRecordWriter<Employee> pinotRecordWriter = PinotOutputFormat.getPinotRecordWriter(taskAttemptContext);
    int numRecords = 10;
    List<Employee> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      Employee employee = new Employee(i, "name" + i, 1000 * i);
      pinotRecordWriter.write(null, employee);
      records.add(employee);
    }
    pinotRecordWriter.close(taskAttemptContext);

    String segmentName = RAW_TABLE_NAME + "_0";
    File segmentDir = new File(TEMP_DIR, "segment");
    File indexDir = TarGzCompressionUtils
        .untar(new File(outputDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION), segmentDir).get(0);
    RecordReader recordReader = new PinotSegmentRecordReader(indexDir, null, null);
    for (Employee record : records) {
      GenericRow row = recordReader.next();
      assertEquals(row.getValue("id"), record.getId());
      assertEquals(row.getValue("name"), record.getName());
      assertEquals(row.getValue("salary"), record.getSalary());
    }
    assertFalse(recordReader.hasNext());
  }

  public static class Employee {
    private final int _id;
    private final String _name;
    private final int _salary;

    private Employee(int id, String name, int salary) {
      _id = id;
      _name = name;
      _salary = salary;
    }

    public int getId() {
      return _id;
    }

    public String getName() {
      return _name;
    }

    public int getSalary() {
      return _salary;
    }
  }
}
