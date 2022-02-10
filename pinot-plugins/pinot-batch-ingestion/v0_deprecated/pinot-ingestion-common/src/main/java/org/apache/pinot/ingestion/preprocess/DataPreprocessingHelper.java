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
package org.apache.pinot.ingestion.preprocess;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pinot.ingestion.utils.InternalConfigConstants;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class DataPreprocessingHelper implements SampleTimeColumnExtractable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataPreprocessingHelper.class);

  public String _partitionColumn;
  public int _numPartitions;
  public String _partitionFunction;
  public String _partitionColumnDefaultNullValue;

  public String _sortingColumn;
  public FieldSpec.DataType _sortingColumnType;
  public String _sortingColumnDefaultNullValue;

  public int _numOutputFiles;
  public int _maxNumRecordsPerFile;

  public TableConfig _tableConfig;
  public Schema _pinotTableSchema;

  public List<Path> _inputDataPaths;
  public Path _sampleRawDataPath;
  public Path _outputPath;

  public DataPreprocessingHelper(List<Path> inputDataPaths, Path outputPath) {
    _inputDataPaths = inputDataPaths;
    _sampleRawDataPath = inputDataPaths.get(0);
    _outputPath = outputPath;
  }

  public void registerConfigs(TableConfig tableConfig, Schema tableSchema, String partitionColumn, int numPartitions,
      String partitionFunction, String partitionColumnDefaultNullValue, String sortingColumn,
      FieldSpec.DataType sortingColumnType, String sortingColumnDefaultNullValue, int numOutputFiles,
      int maxNumRecordsPerFile) {
    _tableConfig = tableConfig;
    _pinotTableSchema = tableSchema;
    _partitionColumn = partitionColumn;
    _numPartitions = numPartitions;
    _partitionFunction = partitionFunction;
    _partitionColumnDefaultNullValue = partitionColumnDefaultNullValue;

    _sortingColumn = sortingColumn;
    _sortingColumnType = sortingColumnType;
    _sortingColumnDefaultNullValue = sortingColumnDefaultNullValue;

    _numOutputFiles = numOutputFiles;
    _maxNumRecordsPerFile = maxNumRecordsPerFile;
  }

  public abstract Class<? extends Partitioner> getPartitioner();

  abstract public Object getSchema(Path inputPathDir)
      throws IOException;

  abstract public void validateConfigsAgainstSchema(Object schema);

  public void setValidationConfigs(Job job, Path path)
      throws IOException {
    SegmentsValidationAndRetentionConfig validationConfig = _tableConfig.getValidationConfig();

    // TODO: Serialize and deserialize validation config by creating toJson and fromJson
    // If the use case is an append use case, check that one time unit is contained in one file.
    // If there is more than one, the job should be disabled, as we should not resize for these use cases.
    // Therefore, setting the time column name and value.
    if (IngestionConfigUtils.getBatchSegmentIngestionType(_tableConfig).equalsIgnoreCase("APPEND")) {
      job.getConfiguration().set(InternalConfigConstants.IS_APPEND, "true");
      String timeColumnName = validationConfig.getTimeColumnName();
      job.getConfiguration().set(InternalConfigConstants.TIME_COLUMN_CONFIG, timeColumnName);
      if (timeColumnName != null) {
        DateTimeFieldSpec dateTimeFieldSpec = _pinotTableSchema.getSpecForTimeColumn(timeColumnName);
        if (dateTimeFieldSpec != null) {
          DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat());
          job.getConfiguration().set(InternalConfigConstants.SEGMENT_TIME_TYPE, formatSpec.getColumnUnit().toString());
          job.getConfiguration()
              .set(InternalConfigConstants.SEGMENT_TIME_FORMAT, formatSpec.getTimeFormat().toString());
          String sdfPattern = formatSpec.getSDFPattern();
          if (sdfPattern != null) {
            job.getConfiguration().set(InternalConfigConstants.SEGMENT_TIME_SDF_PATTERN, formatSpec.getSDFPattern());
          }
        }
      }
      job.getConfiguration().set(InternalConfigConstants.SEGMENT_PUSH_FREQUENCY,
          IngestionConfigUtils.getBatchSegmentIngestionFrequency(_tableConfig));

      String sampleTimeColumnValue = getSampleTimeColumnValue(timeColumnName);
      if (sampleTimeColumnValue != null) {
        job.getConfiguration().set(InternalConfigConstants.TIME_COLUMN_VALUE, sampleTimeColumnValue);
      }
    }
  }
}
