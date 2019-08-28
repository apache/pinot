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
package org.apache.pinot.hadoop.job.mappers;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pinot.core.segment.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.hadoop.job.InternalConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentPreprocessingMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, AvroValue<GenericRecord>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreprocessingMapper.class);
  private String _sortedColumn = null;
  private String _timeColumn = null;
  private Schema _outputKeySchema;
  private Schema _outputSchema;
  private boolean _enablePartitioning;
  private boolean _isAppend = false;
  private String _timeColumnValue;
  private String _pushFrequency;
  private String _timeFormat;
  private TimeUnit _timeUnit;

  @Override
  public void setup(final Context context) {
    Configuration configuration = context.getConfiguration();

    _isAppend = configuration.get(InternalConfigConstants.IS_APPEND).equalsIgnoreCase("true");

    if (_isAppend) {
      // Get time column name
      _timeColumn = configuration.get(InternalConfigConstants.TIME_COLUMN_CONFIG);

      // Get sample time column value
      _timeColumnValue = configuration.get(InternalConfigConstants.TIME_COLUMN_VALUE);

      _pushFrequency = configuration.get(InternalConfigConstants.SEGMENT_PUSH_FREQUENCY);
      String timeType = configuration.get(InternalConfigConstants.SEGMENT_TIME_TYPE);
      _timeFormat = configuration.get(InternalConfigConstants.SEGMENT_TIME_FORMAT);
      _timeUnit = TimeUnit.valueOf(timeType);
    }

    String sortedColumn = configuration.get(InternalConfigConstants.SORTED_COLUMN_CONFIG);
    // Logging the configs for the mapper
    LOGGER.info("Sorted Column: " + sortedColumn);
    if (sortedColumn != null) {
      _sortedColumn = sortedColumn;
    }
    _outputKeySchema = AvroJob.getMapOutputKeySchema(configuration);
    _outputSchema = AvroJob.getMapOutputValueSchema(configuration);
    _enablePartitioning = Boolean.parseBoolean(configuration.get(InternalConfigConstants.ENABLE_PARTITIONING, "false"));
  }

  @Override
  public void map(AvroKey<GenericRecord> record, NullWritable value, final Context context)
      throws IOException, InterruptedException {

    if (_isAppend) {
      // Normalize time column value
      NormalizedDateSegmentNameGenerator normalizedDateSegmentNameGenerator = new NormalizedDateSegmentNameGenerator(_pushFrequency, _timeUnit, _timeFormat);
      String sampleNormalizedTimeColumnValue = normalizedDateSegmentNameGenerator.getNormalizedDate(_timeColumnValue);
      // Normalize time column value and check against sample value
      String timeColumnValue = record.datum().get(_timeColumn).toString();
      String normalizedTimeColumnValue = normalizedDateSegmentNameGenerator.getNormalizedDate(timeColumnValue);

      if (!normalizedTimeColumnValue.equals(sampleNormalizedTimeColumnValue)) {
        // TODO: Create a custom exception and gracefully catch this exception outside, changing what the path to input
        // into segment creation should be
        LOGGER.error("This segment contains multiple time units. Sample is {}, current is {}", sampleNormalizedTimeColumnValue, normalizedTimeColumnValue);
      }
    }

    final GenericRecord inputRecord = record.datum();
    final Schema schema = inputRecord.getSchema();
    Preconditions.checkArgument(_outputSchema.equals(schema), "The schema of all avro files should be the same!");

    GenericRecord outputKey = new GenericData.Record(_outputKeySchema);
    if (_sortedColumn == null) {
      outputKey.put("hashcode", inputRecord.hashCode());
    } else if (_enablePartitioning) {
      outputKey.put(_sortedColumn, inputRecord.get(_sortedColumn));
    } else {
      outputKey.put(_sortedColumn, inputRecord.get(_sortedColumn));
      outputKey.put("hashcode", inputRecord.hashCode());
    }

    try {
      context.write(new AvroKey<>(outputKey), new AvroValue<>(inputRecord));
    } catch (Exception e) {
      LOGGER.error("Exception when writing context on mapper!");
      throw e;
    }
  }
}
