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
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Generic Pinot Output Format implementation.
 */
public class PinotOutputFormat<T> extends FileOutputFormat<NullWritable, T> {

  // Temp directory to create segment
  public static final String TEMP_SEGMENT_DIR = "pinot.temp.segment.dir";

  // Serialized table config
  public static final String TABLE_CONFIG = "pinot.table.config";

  // Serialized schema
  public static final String SCHEMA = "pinot.schema";

  // Class for the field extractor
  public static final String FIELD_EXTRACTOR_CLASS = "pinot.field.extractor.class";

  public static void setTempSegmentDir(Job job, String segmentDir) {
    job.getConfiguration().set(PinotOutputFormat.TEMP_SEGMENT_DIR, segmentDir);
  }

  public static String getTempSegmentDir(JobContext job) {
    return job.getConfiguration().get(PinotOutputFormat.TEMP_SEGMENT_DIR);
  }

  public static void setTableConfig(Job job, TableConfig tableConfig) {
    job.getConfiguration().set(PinotOutputFormat.TABLE_CONFIG, tableConfig.toJsonString());
  }

  public static TableConfig getTableConfig(JobContext job)
      throws IOException {
    return JsonUtils.stringToObject(job.getConfiguration().get(PinotOutputFormat.TABLE_CONFIG), TableConfig.class);
  }

  public static void setSchema(Job job, Schema schema) {
    job.getConfiguration().set(PinotOutputFormat.SCHEMA, schema.toSingleLineJsonString());
  }

  public static Schema getSchema(JobContext job)
      throws IOException {
    return JsonUtils.stringToObject(job.getConfiguration().get(PinotOutputFormat.SCHEMA), Schema.class);
  }

  public static SegmentGeneratorConfig getSegmentGeneratorConfig(JobContext job)
      throws IOException {
    TableConfig tableConfig = getTableConfig(job);
    Schema schema = getSchema(job);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setOutDir(getTempSegmentDir(job) + "/segmentDir");
    segmentGeneratorConfig.setTableName(tableConfig.getTableName());
    segmentGeneratorConfig.setFormat(FileFormat.JSON);
    return segmentGeneratorConfig;
  }

  public static void setFieldExtractorClass(Job job, Class<? extends FieldExtractor> fieldExtractorClass) {
    job.getConfiguration().set(PinotOutputFormat.FIELD_EXTRACTOR_CLASS, fieldExtractorClass.getName());
  }

  public static <T> FieldExtractor<T> getFieldExtractor(JobContext job) {
    Configuration conf = job.getConfiguration();
    try {
      //noinspection unchecked
      return (FieldExtractor<T>) conf.getClassByName(conf.get(PinotOutputFormat.FIELD_EXTRACTOR_CLASS)).newInstance();
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while creating instance of field extractor configured with key: " + FIELD_EXTRACTOR_CLASS);
    }
  }

  public static <T> PinotRecordWriter<T> getPinotRecordWriter(TaskAttemptContext job)
      throws IOException {
    SegmentGeneratorConfig segmentGeneratorConfig = getSegmentGeneratorConfig(job);
    FieldExtractor<T> fieldExtractor = getFieldExtractor(job);
    Set<String> fieldsToRead =
        IngestionUtils.getFieldsForRecordExtractor(segmentGeneratorConfig.getTableConfig().getIngestionConfig(), segmentGeneratorConfig.getSchema());
    fieldExtractor.init(job.getConfiguration(), fieldsToRead);
    return new PinotRecordWriter<>(job, segmentGeneratorConfig, fieldExtractor);
  }

  @Override
  public PinotRecordWriter<T> getRecordWriter(TaskAttemptContext job)
      throws IOException {
    return getPinotRecordWriter(job);
  }
}
