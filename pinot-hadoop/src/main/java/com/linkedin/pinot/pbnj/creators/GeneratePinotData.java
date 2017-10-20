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
package com.linkedin.pinot.pbnj.creators;

import com.google.common.base.Joiner;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.hadoop.ControllerRestApi;
import com.linkedin.pinot.hadoop.job.JobConfigConstants;
import com.linkedin.pinot.hadoop.job.SegmentCreationJob;
import com.linkedin.pinot.hadoop.job.TableConfigConstants;
import com.linkedin.pinot.pbnj.creators.mapper.PbnjSegmentCreationMapper;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GeneratePinotData extends SegmentCreationJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeneratePinotData.class);

  private final String _tableName;
  private final String _pushLocations;
  private final String _segmentNameKey;

  public GeneratePinotData(String name, Properties properties) throws Exception {
    super(name, properties);
    _pushLocations = properties.getProperty(JobConfigConstants.PUSH_LOCATION);
    _tableName = properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME);
    _segmentNameKey = properties.getProperty(JobConfigConstants.SEGMENT_NAME_KEY);
    LOGGER.info("Push locations: " + _pushLocations);
    LOGGER.info("Table Name " + _tableName);
  }

  @Override
  protected String getOutputDir() {
    Path inputPath = new Path(getInputDir());
    LOGGER.info("Using input path: " + inputPath);

    LOGGER.info("Parent directory of input used to construct output directory: " + inputPath.getParent());
    String outputPath = inputPath.getParent().toString() + "/pinot_segments";
    LOGGER.info("Using output path: " + outputPath);
    return outputPath;
  }

  @Override
  protected void setOutputPath(Configuration configuration) {
    LOGGER.info("Setting " + JobConfigConstants.PATH_TO_OUTPUT + " to: " + getOutputDir());
    configuration.set(JobConfigConstants.PATH_TO_OUTPUT, getOutputDir());
  }

  @Override
  protected void setAdditionalJobProperties(Job job) throws Exception {
    ControllerRestApi controllerRestApiObject = new ControllerRestApi(_pushLocations);

    TableConfig tableConfig = controllerRestApiObject.getTableConfig(_tableName);
    job.getConfiguration()
        .set(JobConfigConstants.TABLE_PUSH_TYPE, tableConfig.getValidationConfig().getSegmentPushType());
    if (tableConfig.getValidationConfig().getSegmentPushType().equalsIgnoreCase(TableConfigConstants.APPEND)) {
      job.getConfiguration()
          .set(JobConfigConstants.TIME_COLUMN_NAME, tableConfig.getValidationConfig().getTimeColumnName());
      job.getConfiguration().set(JobConfigConstants.TIME_COLUMN_TYPE, tableConfig.getValidationConfig().getTimeType());
    } else {
      LOGGER.info("Refresh use case. Not setting timeColumnName and timeColumnType: " + _tableName);
    }

    String schema = controllerRestApiObject.getSchema(_tableName);
    LOGGER.info("Setting schema for tableName " + _tableName + "to: " + schema);
    job.getConfiguration().set("data.schema", schema);

    // TODO: Handle split logic inside TimeGranularitySpec
    Schema serializedSchema;
    try {
      serializedSchema = Schema.fromString(schema);
    } catch (Exception e) {
      throw new RuntimeException("Could not serialize schema " + schema, e);
    }
    if (tableConfig.getValidationConfig().getSegmentPushType().equalsIgnoreCase(TableConfigConstants.APPEND)) {
      configTimeFormat(serializedSchema, job.getConfiguration());
    }

    job.getConfiguration()
        .set(JobConfigConstants.TABLE_PUSH_FREQUENCY, tableConfig.getValidationConfig().getSegmentPushFrequency());
    String noDictionaryColumnString = "";
    List<String> noDictionaryColumns = tableConfig.getIndexingConfig().getNoDictionaryColumns();
    if (noDictionaryColumns != null && noDictionaryColumns.size() > 0) {
      LOGGER.info(
          "No Dictionary Columns are: " + tableConfig.getIndexingConfig().getNoDictionaryColumns() + " tableName: "
              + _tableName);
      noDictionaryColumnString = Joiner.on(JobConfigConstants.SEPARATOR).join(noDictionaryColumns);
      job.getConfiguration().set(JobConfigConstants.TABLE_RAW_INDEX_COLUMNS, noDictionaryColumnString);
    }
    SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (segmentPartitionConfig != null) {
      LOGGER.info(
          "Segment Partition Config is: " + segmentPartitionConfig.toJsonString() + " for table name: " + _tableName);
      job.getConfiguration().set(JobConfigConstants.SEGMENT_PARTITIONERS, segmentPartitionConfig.toJsonString());
    }

    if (tableConfig.getCustomConfig() != null) {
      Map<String, String> customConfigs = tableConfig.getCustomConfig().getCustomConfigs();
      String segmentNamePrefix = customConfigs.get(TableConfigConstants.SEGMENT_PREFIX);
      if (segmentNamePrefix != null) {
        // Check that there is no key that begins with segmentname.keyedprefix
        for (String key : customConfigs.keySet()) {
          if (key.startsWith(TableConfigConstants.SEGMENT_NAME_KEYED_PREFIX)) {
            throw new RuntimeException("Both" + TableConfigConstants.SEGMENT_NAME_KEYED_PREFIX + _segmentNameKey + " and "
                + TableConfigConstants.SEGMENT_PREFIX + " cannot be set at the same time" + " for table name: " + _tableName);
          }
        }
        // Set for use cases for which all segments have the same prefix different from default, eg, segmentNamePrefix = mirrorShareEvents_daily
        LOGGER.info("Segment Name Default Prefix is: " + segmentNamePrefix + " for table name: " + _tableName);
        job.getConfiguration().set(JobConfigConstants.SEGMENT_PREFIX, segmentNamePrefix);
      }

      if (_segmentNameKey != null) {
        String segmentNameKeyedPrefix = customConfigs.get(TableConfigConstants.SEGMENT_NAME_KEYED_PREFIX + _segmentNameKey);
        if (segmentNameKeyedPrefix != null) {
          // Set for use cases where segments in a table can have different prefixes,
          // eg, prefix would be set to either of the following: adsOfflineEvents_adsOfflineEvents_daily, adsOfflineEvents_adsOfflineEvents_monthly
          LOGGER.info("Segment Prefix is " + segmentNameKeyedPrefix + " for table name: " + _tableName + "," + "segment name key is: " + _segmentNameKey);
          job.getConfiguration().set(JobConfigConstants.SEGMENT_PREFIX, segmentNameKeyedPrefix);
        } else {
          // Throws an exception if user sets segment prefix key, but it isn't present in table config.
          LOGGER.info("Segment name key set to " + _segmentNameKey + " in the hadoop job, but table config doesn't set "
              + TableConfigConstants.SEGMENT_NAME_KEYED_PREFIX + _segmentNameKey + " for table name: " + _tableName);
          throw new RuntimeException(
              "Segment name key set to " + _segmentNameKey + " but table config doesn't have this set" + " for table name: " + _tableName);
        }
      }

      String excludeSequenceId = customConfigs.get(TableConfigConstants.SEGMENT_EXCLUDE_SEQUENCE_ID);
      if (excludeSequenceId != null) {
        LOGGER.warn("Exclude sequence Id set to " + excludeSequenceId + " for table name: " + _tableName);
        job.getConfiguration().set(TableConfigConstants.SEGMENT_EXCLUDE_SEQUENCE_ID, excludeSequenceId);
      }

      // Check the table config to determine whether we want to generate inverted indices for a specific use case before push
      boolean generateInvertedIndexBeforePush = false;
      try {
        generateInvertedIndexBeforePush = Boolean.parseBoolean(tableConfig.getCustomConfig().getCustomConfigs()
            .get(TableConfigConstants.GENERATE_INVERTED_INDEX_BEFORE_PUSH));
      } catch (Exception e) {
        LOGGER.info("Not generating inverted indices before push");
      }
      if (generateInvertedIndexBeforePush) {
        // Inverted Index Columns
        String invertedIndexColumnString = "";
        List<String> invertedIndexColumns = tableConfig.getIndexingConfig().getInvertedIndexColumns();
        if (invertedIndexColumns != null && invertedIndexColumns.size() > 0) {
          LOGGER.info("Inverted Index Columns are: " + tableConfig.getIndexingConfig().getInvertedIndexColumns() + " tableName: "
              + _tableName);
          invertedIndexColumnString = Joiner.on(JobConfigConstants.SEPARATOR).join(invertedIndexColumns);
          job.getConfiguration().set(JobConfigConstants.TABLE_INVERTED_INDEX_COLUMNS, invertedIndexColumnString);
        }
      }
    }
  }

  private void configTimeFormat(Schema schema, Configuration configuration) {
    String timeFormatStr = schema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeFormat();

    // Validate Time Format
    if (timeFormatStr.equals(TimeGranularitySpec.TimeFormat.EPOCH.toString())) {
      configuration.set(JobConfigConstants.SCHEMA_TIME_FORMAT, timeFormatStr);
      return;
    }
    if (!timeFormatStr.startsWith(TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString())
        && !timeFormatStr.startsWith(TimeGranularitySpec.TimeFormat.EPOCH.toString())) {
      throw new RuntimeException("Time format has to be one of '" + TimeGranularitySpec.TimeFormat.EPOCH + "' or " + TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT + "':<formatString>'");
    }
    String simpleDateTypeStr = timeFormatStr.split(":")[1];
    // Throws illegal argument exception if invalid
    try {
      new SimpleDateFormat(simpleDateTypeStr);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Could not parse simple date format " + simpleDateTypeStr, e);
    }
    configuration.set(JobConfigConstants.SCHEMA_TIME_FORMAT, simpleDateTypeStr);
    return;
  }

  @Override
  protected Job setMapperClass(Job job) {
    job.setMapperClass(PbnjSegmentCreationMapper.class);
    return job;
  }
}
