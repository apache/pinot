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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;


/**
 * Generic Pinot Output Format implementation.
 *
 * TODO: Support star-tree creation
 */
public class PinotOutputFormat<K, V> extends FileOutputFormat<K, V> {

  private final SegmentGeneratorConfig _segmentConfig;

  // Pinot temp directory to create segment.
  public static final String TEMP_SEGMENT_DIR = "pinot.temp.segment.dir";

  // Name of the table
  public static final String TABLE_NAME = "pinot.table.name";

  // Name of the segment.
  public static final String SEGMENT_NAME = "pinot.segment_name";

  // Name of the time column.
  public static final String TIME_COLUMN_NAME = "pinot.time_column_name";

  // file containing schema for the data
  public static final String SCHEMA = "pinot.schema.file";

  public static final String PINOT_RECORD_SERIALIZATION_CLASS = "pinot.record.serialization.class";

  public PinotOutputFormat() {
    _segmentConfig = new SegmentGeneratorConfig();
  }

  public static void setTempSegmentDir(Job job, String segmentDir) {
    job.getConfiguration().set(PinotOutputFormat.TEMP_SEGMENT_DIR, segmentDir);
  }

  public static String getTempSegmentDir(JobContext job) {
    return job.getConfiguration().get(PinotOutputFormat.TEMP_SEGMENT_DIR, ".data_" + getTableName(job));
  }

  public static void setTableName(Job job, String table) {
    job.getConfiguration().set(PinotOutputFormat.TABLE_NAME, table);
  }

  public static String getTableName(JobContext job) {
    String table = job.getConfiguration().get(PinotOutputFormat.TABLE_NAME);
    if (table == null) {
      throw new RuntimeException("pinot table name not set.");
    }
    return table;
  }

  public static void setSegmentName(Job job, String segmentName) {
    job.getConfiguration().set(PinotOutputFormat.SEGMENT_NAME, segmentName);
  }

  public static String getSegmentName(JobContext context) {
    String segment = context.getConfiguration().get(PinotOutputFormat.SEGMENT_NAME);
    if (segment == null) {
      throw new RuntimeException("pinot segment name not set.");
    }
    return segment;
  }

  public static void setTimeColumnName(Job job, String timeColumnName) {
    job.getConfiguration().set(PinotOutputFormat.TIME_COLUMN_NAME, timeColumnName);
  }

  public static String getTimeColumnName(JobContext context) {
    return context.getConfiguration().get(PinotOutputFormat.TIME_COLUMN_NAME);
  }

  public static void setSchema(Job job, Schema schema) {
    job.getConfiguration().set(PinotOutputFormat.SCHEMA, schema.toSingleLineJsonString());
  }

  public static String getSchema(JobContext context) {
    String schemaFile = context.getConfiguration().get(PinotOutputFormat.SCHEMA);
    if (schemaFile == null) {
      throw new RuntimeException("pinot schema file not set");
    }
    return schemaFile;
  }

  public static void setDataWriteSupportClass(Job job, Class<? extends PinotRecordSerialization> pinotSerialization) {
    job.getConfiguration().set(PinotOutputFormat.PINOT_RECORD_SERIALIZATION_CLASS, pinotSerialization.getName());
  }

  public static Class<?> getDataWriteSupportClass(JobContext context) {
    String className = context.getConfiguration().get(PinotOutputFormat.PINOT_RECORD_SERIALIZATION_CLASS);
    if (className == null) {
      throw new RuntimeException("pinot data write support class not set");
    }
    try {
      return context.getConfiguration().getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    configure(context.getConfiguration());
    final PinotRecordSerialization dataWriteSupport = getDataWriteSupport(context);
    initSegmentConfig(context);
    Path workDir = getDefaultWorkFile(context, "");
    return new PinotRecordWriter<>(_segmentConfig, context, workDir, dataWriteSupport);
  }

  /**
   * The {@link #configure(Configuration)} method called before initialize the  {@link
   * RecordWriter} Any implementation of {@link PinotOutputFormat} can use it to set additional
   * configuration properties.
   */
  public void configure(Configuration conf) {

  }

  private PinotRecordSerialization getDataWriteSupport(TaskAttemptContext context) {
    try {
      return (PinotRecordSerialization) PinotOutputFormat.getDataWriteSupportClass(context).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Error initialize data write support class", e);
    }
  }

  private void initSegmentConfig(JobContext context)
      throws IOException {
    _segmentConfig.setFormat(FileFormat.JSON);
    _segmentConfig.setOutDir(PinotOutputFormat.getTempSegmentDir(context) + "/segmentDir");
    _segmentConfig.setTableName(PinotOutputFormat.getTableName(context));
    _segmentConfig.setSegmentName(PinotOutputFormat.getSegmentName(context));
    Schema schema = Schema.fromString(PinotOutputFormat.getSchema(context));
    _segmentConfig.setSchema(schema);
    _segmentConfig.setTime(PinotOutputFormat.getTimeColumnName(context), schema);
  }
}
