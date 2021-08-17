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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.pinot.hadoop.job.InternalConfigConstants;
import org.apache.pinot.hadoop.utils.preprocess.DataPreprocessingUtils;
import org.apache.pinot.hadoop.utils.preprocess.OrcUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OrcDataPreprocessingMapper extends Mapper<NullWritable, OrcStruct, WritableComparable, OrcValue> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrcDataPreprocessingMapper.class);

  private final OrcValue _valueWrapper = new OrcValue();
  private String _sortingColumn = null;
  private FieldSpec.DataType _sortingColumnType = null;
  private int _sortingColumnId = -1;

  @Override
  public void setup(Context context) {
    Configuration configuration = context.getConfiguration();
    String sortingColumnConfig = configuration.get(InternalConfigConstants.SORTING_COLUMN_CONFIG);
    if (sortingColumnConfig != null) {
      _sortingColumn = sortingColumnConfig;
      _sortingColumnType = FieldSpec.DataType.valueOf(configuration.get(InternalConfigConstants.SORTING_COLUMN_TYPE));
      LOGGER.info("Initialized OrcDataPreprocessingMapper with sortingColumn: {} of type: {}", _sortingColumn, _sortingColumnType);
    } else {
      LOGGER.info("Initialized OrcDataPreprocessingMapper without sorting column");
    }
  }

  @Override
  public void map(NullWritable key, OrcStruct value, Context context)
      throws IOException, InterruptedException {
    _valueWrapper.value = value;
    if (_sortingColumn != null) {
      if (_sortingColumnId == -1) {
        List<String> fieldNames = value.getSchema().getFieldNames();
        _sortingColumnId = fieldNames.indexOf(_sortingColumn);
        Preconditions.checkState(_sortingColumnId != -1, "Failed to find sorting column: %s in the ORC fields: %s", _sortingColumn, fieldNames);
        LOGGER.info("Field id for sorting column: {} is: {}", _sortingColumn, _sortingColumnId);
      }
      WritableComparable sortingColumnValue = value.getFieldValue(_sortingColumnId);
      WritableComparable outputKey;
      try {
        outputKey = DataPreprocessingUtils.convertToWritableComparable(OrcUtils.convert(sortingColumnValue), _sortingColumnType);
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format("Caught exception while processing sorting column: %s, id: %d in ORC struct: %s", _sortingColumn, _sortingColumnId, value), e);
      }
      context.write(outputKey, _valueWrapper);
    } else {
      context.write(NullWritable.get(), _valueWrapper);
    }
  }
}
