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
package com.linkedin.pinot.pbnj.creators.mapper;

import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.DefaultSegmentNameGenerator;
import com.linkedin.pinot.core.segment.SegmentNameGenerator;
import com.linkedin.pinot.hadoop.job.JobConfigConstants;
import com.linkedin.pinot.hadoop.job.TableConfigConstants;
import com.linkedin.pinot.hadoop.job.mapper.HadoopSegmentCreationMapReduceJob;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PbnjSegmentCreationMapper extends HadoopSegmentCreationMapReduceJob.HadoopSegmentCreationMapper {

  private static Logger LOGGER = LoggerFactory.getLogger(PbnjSegmentCreationMapper.class);

  private String _timeColumnName;
  private String _timeColumnType;
  private String _tablePushType;
  private String _tablePushFrequency;
  private String _segmentNamePrefix;
  private String _segmentExcludeSequenceId;
  private String _schemaTimeFormat;

  private String _rawIndexColumns;
  private List<String> _rawIndexColumnList;

  private String _segmentPartitioners;
  private Logger segmentLogger = LOGGER;

  private String _invertedIndexColumns;
  private List<String> _invertedIndexColumnList;

  private static Thread _progressReporterThread;

  @Override
  public void cleanup(Context context)
      throws IOException, InterruptedException {
  }

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration properties = context.getConfiguration();
    _timeColumnName = properties.get(JobConfigConstants.TIME_COLUMN_NAME, null);
    _timeColumnType = properties.get(JobConfigConstants.TIME_COLUMN_TYPE, null);
    _tablePushType = properties.get(JobConfigConstants.TABLE_PUSH_TYPE, null);
    _tablePushFrequency = properties.get(JobConfigConstants.TABLE_PUSH_FREQUENCY, null);
    _segmentPartitioners = properties.get(JobConfigConstants.SEGMENT_PARTITIONERS, null);
    _segmentNamePrefix = properties.get(JobConfigConstants.SEGMENT_PREFIX, null);
    _segmentExcludeSequenceId = properties.get(JobConfigConstants.SEGMENT_EXCLUDE_SEQUENCE_ID, null);
    _schemaTimeFormat = properties.get(JobConfigConstants.SCHEMA_TIME_FORMAT, null);

    if (_tablePushType.equalsIgnoreCase(TableConfigConstants.APPEND) && _timeColumnName == null) {
      // We need to use the time column to generate segment names that have date in them.
      throw new RuntimeException("There needs to be a time column configured for append use cases for table " + getTableName());
    }

    if (_tablePushType.equalsIgnoreCase(TableConfigConstants.REFRESH) && _timeColumnName != null) {
      // We need to use the time column to generate segment names that have date in them.
      throw new RuntimeException("Refresh use cases should not have a time column configured for table " + getTableName());
    }

    _rawIndexColumns = properties.get(JobConfigConstants.TABLE_RAW_INDEX_COLUMNS, null);

    if (_rawIndexColumns != null) {
      _rawIndexColumnList = Arrays.asList(properties.get(JobConfigConstants.TABLE_RAW_INDEX_COLUMNS, null).split(
          JobConfigConstants.SEPARATOR));
      LOGGER.info("rawIndexColumnList: " + _rawIndexColumnList);
    }

    _invertedIndexColumns = properties.get(JobConfigConstants.TABLE_INVERTED_INDEX_COLUMNS, null);
    if (_invertedIndexColumns != null) {
      _invertedIndexColumnList = Arrays.asList(properties.get(JobConfigConstants.TABLE_INVERTED_INDEX_COLUMNS, null).split(
          JobConfigConstants.SEPARATOR));
      LOGGER.info("invertedIndexColumnList: " + _invertedIndexColumnList);
    }

    ProgressReporter progressReporter = new ProgressReporter(context);
    _progressReporterThread =  new Thread(progressReporter);
    _progressReporterThread.setName("pinot-hadoop-progress-reporter");
    super.setup(context);
    segmentLogger = LoggerFactory.getLogger(PbnjSegmentCreationMapper.class.getName() +
        "_" + getTableName());

    segmentLogger.info("Time Column Name: " + _timeColumnName
        + ",Time Column Type: " + _timeColumnType
        + ",Table Push Type: " + _tablePushType
        + ",Table Push Frequency: " + _tablePushFrequency
        + ",Segment Partitioners: " + _segmentPartitioners
        + ",Segment Name Prefix: " + _segmentNamePrefix);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    final int maxWaitSeconds = 5;

    /*
     * PINOT-1788
     * Start a thread that reports progress every minute throughout the job.
     * Otherwise the job gets killed in 10 minutes since we don't write output
     * from the mapper.
     */
    _progressReporterThread.start();
    try {
      super.map(key, value, context);
    } finally {
      _progressReporterThread.interrupt();
      // Wait for at most maxWaitSeconds before reporting an error. The thread should die by that time.
      _progressReporterThread.join(maxWaitSeconds * 1000L);
      if (_progressReporterThread.isAlive()) {
        segmentLogger.error("Could not kill progress report thread:" + _progressReporterThread);
      }
    }
  }

  @Override
  protected void setSegmentNameGenerator(SegmentGeneratorConfig segmentGeneratorConfig, Integer seqId, Path hdfsAvroPath, File dataPath) {
    SegmentNameGenerator segmentNameGenerator = new DefaultSegmentNameGenerator(_timeColumnName, getTableName(), null, seqId);
    segmentGeneratorConfig.setSegmentNameGenerator(segmentNameGenerator);
    segmentGeneratorConfig.setTableName(getTableName());
    segmentGeneratorConfig.setInputFilePath(new File(dataPath, hdfsAvroPath.getName()).getAbsolutePath());
    segmentGeneratorConfig.setFormat(FileFormat.AVRO);
    if (_rawIndexColumnList != null) {
      segmentLogger.info("Raw Index Column List is: " + _rawIndexColumnList.toString());
      segmentGeneratorConfig.setRawIndexCreationColumns(_rawIndexColumnList);
    }
    if (_segmentPartitioners != null) {
      segmentLogger.info("Setting segment partitioners to: " + _segmentPartitioners);
      try {
        segmentGeneratorConfig.setSegmentPartitionConfig(SegmentPartitionConfig.fromJsonString(_segmentPartitioners));
      } catch (IOException e) {
        segmentLogger.error("Could not convert segment partition config from json to SegmentPartitionConfig: " + _segmentPartitioners);
      }
    }
    if (_invertedIndexColumnList != null) {
      segmentLogger.info("Setting inverted index columns to: " + _invertedIndexColumnList.toString());
      segmentGeneratorConfig.setInvertedIndexCreationColumns(_invertedIndexColumnList);
    }
  }

  private static class ProgressReporter implements Runnable {
    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ProgressReporter.class);
    private static final int intervalSec = 60;  // time period between progress reports.

    private final Mapper.Context context;

    ProgressReporter(Mapper.Context context) {
      this.context = context;
    }
    @Override
    public void run() {

      logger.info("Starting Progress reporter thread " + Thread.currentThread());
      while (true) {
        try {
          Thread.sleep(intervalSec * 1000L);
          logger.info("============== Reporting progresss ========== ");
          context.progress();
          // In case the interrupt came in while we were reporting progress.
          if (Thread.currentThread().isInterrupted()) {
            logger.info("Exiting progress reporter (Interrupted while reporting progress):" + Thread.currentThread());
            return;
          }
        } catch (InterruptedException e) {
          logger.info("Exiting progress reporter (Interrupted while sleeping):" + Thread.currentThread());
          return;
        }
      }
    }
  }
}
