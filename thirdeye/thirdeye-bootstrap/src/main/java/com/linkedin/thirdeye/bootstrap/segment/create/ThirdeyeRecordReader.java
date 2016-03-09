/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.thirdeye.bootstrap.segment.create;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.BaseRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReaderUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.mapred.Pair;
import org.apache.avro.mapred.SequenceFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ThirdeyeRecordReader extends BaseRecordReader {
  private static final Logger _logger = LoggerFactory.getLogger(ThirdeyeRecordReader.class);

  private Schema _schema = null;
  private File _sequenceFile;
  private StarTreeConfig _starTreeConfig;

  private Map<String, Integer> _dimensionToIndexMapping;
  private Map<String, Integer> _metricToIndexMapping;

  private SequenceFileReader<ByteBuffer, ByteBuffer> _sequenceFileReader;
  private MetricSchema _metricSchema;
  private String[] _dimensionValues = null;
  private Map<Long, List<Number>> _metricBuffer = null;
  private Iterator<Entry<Long, List<Number>>> _metricTimeSeriesIterator;

  private boolean countIncluded;


  public ThirdeyeRecordReader(String sequenceFileName, Schema schema, String starTreeConfigFileName) throws IOException {
    super();
    super.initNullCounters(schema);
    _sequenceFile = new File(sequenceFileName);
    _schema = schema;

    FileSystem fs = FileSystem.get(new Configuration());
    _starTreeConfig = StarTreeConfig.decode(fs.open(new Path(starTreeConfigFileName)));

    _dimensionToIndexMapping = new HashMap<>();
    for (int i = 0; i < _starTreeConfig.getDimensions().size(); i ++) {
      _dimensionToIndexMapping.put(_starTreeConfig.getDimensions().get(i).getName(), i);
    }

    _metricToIndexMapping = new HashMap<>();
    for (int i = 0; i < _starTreeConfig.getMetrics().size(); i ++) {
      _metricToIndexMapping.put(_starTreeConfig.getMetrics().get(i).getName(), i);
    }

    countIncluded = false;
    for (String metricName : _starTreeConfig.getMetricNames()) {
      if (metricName.equals(StarTreeConstants.AUTO_METRIC_COUNT)) {
        countIncluded = true;
        break;
      }
    }

    _metricSchema = MetricSchema.fromMetricSpecs(_starTreeConfig.getMetrics());

  }

  @Override
  public void init() throws Exception {
    _sequenceFileReader = new SequenceFileReader<ByteBuffer, ByteBuffer>(_sequenceFile);
    _metricTimeSeriesIterator = null;
  }

  @Override
  public void rewind() throws Exception {
    _sequenceFileReader.close();
    init();
  }

  @Override
  public boolean hasNext() {
    return (_metricTimeSeriesIterator != null && _metricTimeSeriesIterator.hasNext())
        || _sequenceFileReader.hasNext();
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public GenericRow next() {

    if (_metricTimeSeriesIterator == null || !_metricTimeSeriesIterator.hasNext()) {

      Pair<ByteBuffer, ByteBuffer> sequenceRecord = _sequenceFileReader.next();

      try {

        byte[] keyBytes = sequenceRecord.key().array();
        DimensionKey dimensionKey = DimensionKey.fromBytes(keyBytes);
        _dimensionValues = dimensionKey.getDimensionValues();

        byte[] valueBytes = sequenceRecord.value().array();
        MetricTimeSeries metricTimeSeries = MetricTimeSeries.fromBytes(valueBytes, _metricSchema);
        _metricBuffer = new HashMap<>();

        for (Long time : metricTimeSeries.getTimeWindowSet()) {
          List<Number> timeSeries = new ArrayList<>();
          for (MetricSpec metricSpec : _starTreeConfig.getMetrics()) {
            timeSeries.add(metricTimeSeries.get(time, metricSpec.getName()));
          }
          _metricBuffer.put(time, timeSeries);
        }
        _metricTimeSeriesIterator = _metricBuffer.entrySet().iterator();

      } catch (Exception e) {
        _logger.error("Exception while reading next thirdeye sequence record", e);
      }

    }

    Map<String, Object> fieldMap = new HashMap<String, Object>();

    for (DimensionSpec dimensionSpec : _starTreeConfig.getDimensions()) {
      String dimensionName = dimensionSpec.getName();
      String dimensionValue = _dimensionValues[_dimensionToIndexMapping.get(dimensionName)];
      fieldMap.put(dimensionName, dimensionValue);
    }

    Entry<Long, List<Number>> metricTimeSeries = _metricTimeSeriesIterator.next();
    List<Number> metricTimeSeriesValues = metricTimeSeries.getValue();
    for (MetricSpec metricSpec : _starTreeConfig.getMetrics()) {
      String metricName = metricSpec.getName();
      String metricValue = metricTimeSeriesValues.get(_metricToIndexMapping.get(metricName)).toString();
      DataType dataType = DataType.valueOf(metricSpec.getType().toString());
      fieldMap.put(metricName, RecordReaderUtils.convertToDataType(metricValue, dataType));
    }
    if (!countIncluded) {
      fieldMap.put(StarTreeConstants.AUTO_METRIC_COUNT, 1);
    }

    fieldMap.put(_starTreeConfig.getTime().getColumnName(), metricTimeSeries.getKey());

    GenericRow genericRow = new GenericRow();
    genericRow.init(fieldMap);
    return genericRow;
  }

  @Override
  public void close() throws Exception {
    _sequenceFileReader.close();
  }

}
