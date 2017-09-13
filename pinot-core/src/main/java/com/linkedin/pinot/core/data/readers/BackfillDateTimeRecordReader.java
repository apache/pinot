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
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.DateTimeFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.IOException;
import org.apache.commons.configuration.ConfigurationException;


/**
 * This record reader is a wrapper over another record reader.
 * It simply reads the records from the base record reader, and adds a new field according to the dateTimeFieldSpec
 */
public class BackfillDateTimeRecordReader extends BaseRecordReader {

  private RecordReader _baseRecordReader;
  private TimeFieldSpec _timeFieldSpec;
  private DateTimeFieldSpec _dateTimeFieldSpec;
  private int _numRows = Integer.MAX_VALUE;
  private int _nextRow;

  /**
   * Constructor to read from the base record reader, but limit the number of rows read
   * This is useful when backfilling star tree segments, so that we read only total raw docs
   * @param baseRecordReader
   * @param timeFieldSpec
   * @param dateTimeFieldSpec
   * @param maxNumRows
   * @throws IOException
   * @throws ConfigurationException
   */
  public BackfillDateTimeRecordReader(RecordReader baseRecordReader, TimeFieldSpec timeFieldSpec,
      DateTimeFieldSpec dateTimeFieldSpec, int maxNumRows) throws IOException, ConfigurationException {
    this(baseRecordReader, timeFieldSpec, dateTimeFieldSpec);
    _numRows = maxNumRows;
  }

  public BackfillDateTimeRecordReader(RecordReader baseRecordReader, TimeFieldSpec timeFieldSpec,
      DateTimeFieldSpec dateTimeFieldSpec) throws IOException, ConfigurationException {
    _baseRecordReader = baseRecordReader;
    _timeFieldSpec = timeFieldSpec;
    _dateTimeFieldSpec = dateTimeFieldSpec;
    _nextRow = 0;
  }

  @Override
  public void init() throws Exception {
    _baseRecordReader.init();
    _nextRow = 0;
  }

  @Override
  public void rewind() throws Exception {
    _baseRecordReader.rewind();
    _nextRow = 0;
  }

  @Override
  public boolean hasNext() {
    return _nextRow < _numRows && _baseRecordReader.hasNext();
  }

  /**
   * Reads the schema from the baseRecordReader and adds/updates dateTimeFieldSpec to it, if not already present
   * {@inheritDoc}
   * @see com.linkedin.pinot.core.data.readers.RecordReader#getSchema()
   */
  @Override
  public Schema getSchema() {
    Schema schema = _baseRecordReader.getSchema();
    // base record reader doesn't have _dateTimeFieldSpec
    if (schema.getDateTimeNames() == null || !schema.getDateTimeNames().contains(_dateTimeFieldSpec.getName())) {
      schema.addField(_dateTimeFieldSpec);
    } // base record reader has dateTimeFieldSpec, but with different configs
    // create schema with update dateTimeFieldSpec
    else if (!_dateTimeFieldSpec.equals(schema.getDateTimeSpec(_dateTimeFieldSpec.getName()))) {
      Schema newSchema = new Schema();
      for (DimensionFieldSpec dimensionFieldSpec : schema.getDimensionFieldSpecs()) {
        newSchema.addField(dimensionFieldSpec);
      }
      for (MetricFieldSpec metricFieldSpec : schema.getMetricFieldSpecs()) {
        newSchema.addField(metricFieldSpec);
      }
      newSchema.addField(schema.getTimeFieldSpec());
      newSchema.addField(_dateTimeFieldSpec);
      newSchema.setSchemaName(schema.getSchemaName());
      return newSchema;
    }
    return schema;
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  /**
   * Reads the next row from the baseRecordReader, and adds a dateTimeFieldSPec column to it
   * {@inheritDoc}
   * @see com.linkedin.pinot.core.data.readers.RecordReader#next(com.linkedin.pinot.core.data.GenericRow)
   */
  @Override
  public GenericRow next(GenericRow row) {
    _baseRecordReader.next(row);
    Long timeColumnValue = (Long) row.getValue(_timeFieldSpec.getIncomingTimeColumnName());
    Object dateTimeColumnValue = convertTimeFieldToDateTimeFieldSpec(timeColumnValue);
    row.putField(_dateTimeFieldSpec.getName(), dateTimeColumnValue);
    _nextRow ++;
    return row;
  }

  /**
   * Converts the time column value from timeFieldSpec to dateTimeFieldSpec
   * @param timeColumnValue - time column value from timeFieldSpec
   * @return
   */
  private Object convertTimeFieldToDateTimeFieldSpec(Object timeColumnValue) {

    TimeGranularitySpec timeGranularitySpec = _timeFieldSpec.getIncomingGranularitySpec();
    String formatFromTimeSpec = DateTimeFieldSpec.constructFormat(
        timeGranularitySpec.getTimeUnitSize(), timeGranularitySpec.getTimeType(), timeGranularitySpec.getTimeFormat());
    if (formatFromTimeSpec.equals(_dateTimeFieldSpec.getFormat())) {
      return timeColumnValue;
    }

    long timeColumnValueMS = timeGranularitySpec.toMillis(timeColumnValue);
    Object dateTimeColumnValue = _dateTimeFieldSpec.fromMillis(timeColumnValueMS);
    return dateTimeColumnValue;
  }

  @Override
  public void close() throws Exception {
    _baseRecordReader.close();
  }

}
