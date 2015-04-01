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
package com.linkedin.pinot.core.realtime.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.HashUtil;
import com.linkedin.pinot.common.utils.time.TimeConverter;
import com.linkedin.pinot.common.utils.time.TimeConverterProvider;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.realtime.RealtimeSegment;
import com.linkedin.pinot.core.realtime.impl.datasource.RealtimeColumnDataSource;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.realtime.impl.dictionary.RealtimeDictionaryProvider;
import com.linkedin.pinot.core.realtime.impl.fwdindex.DimensionTuple;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.DimensionInvertertedIndex;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.MetricInvertedIndex;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.RealtimeInvertedIndex;
import com.linkedin.pinot.core.realtime.impl.invertedIndex.TimeInvertedIndex;
import com.linkedin.pinot.core.realtime.utils.RealtimeDimensionsSerDe;
import com.linkedin.pinot.core.realtime.utils.RealtimeMetricsSerDe;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


public class RealtimeSegmentImpl implements RealtimeSegment {
  private SegmentMetadataImpl _segmentMetadata;
  private final Schema dataSchema;

  private String segmentName;

  private final Map<String, MutableDictionaryReader> dictionaryMap;
  private final Map<Long, DimensionTuple> dimemsionTupleMap;
  private final Map<String, RealtimeInvertedIndex> invertedIndexMap;

  private final TimeConverter timeConverter;
  private AtomicInteger docIdGenerator;
  private String incomingTimeColumnName;
  private String outgoingTimeColumnName;

  private Map<Object, Pair<Long, Object>> docIdMap;
  private Map<String, Integer> maxNumberOfMultivaluesMap;

  private final RealtimeDimensionsSerDe dimensionsSerde;
  private final RealtimeMetricsSerDe metricsSerDe;

  private int docIdSearchableOffset = 0;
  private int numDocsIndexed = 0;
  private int numSuccessIndexed = 0;

  // to compute the rolling interval
  private long minTimeVal = Long.MAX_VALUE;
  private long maxTimeVal = Long.MIN_VALUE;

  public RealtimeSegmentImpl(Schema schema) {
    dataSchema = schema;
    dictionaryMap = new HashMap<String, MutableDictionaryReader>();
    maxNumberOfMultivaluesMap = new HashMap<String, Integer>();

    for (String column : dataSchema.getDimensionNames()) {
      maxNumberOfMultivaluesMap.put(column, 0);
      dictionaryMap.put(column, RealtimeDictionaryProvider.getDictionaryFor(dataSchema.getFieldSpecFor(column)));
    }

    docIdGenerator = new AtomicInteger(-1);

    dimemsionTupleMap = new HashMap<Long, DimensionTuple>();

    timeConverter =
        TimeConverterProvider.getTimeConverterFromGranularitySpecs(schema.getTimeSpec().getIncominGranularutySpec(),
            schema.getTimeSpec().getOutgoingGranularitySpec());

    incomingTimeColumnName = dataSchema.getTimeSpec().getIncomingTimeColumnName();
    outgoingTimeColumnName = dataSchema.getTimeSpec().getOutGoingTimeColumnName();
    dictionaryMap.put(outgoingTimeColumnName,
        RealtimeDictionaryProvider.getDictionaryFor(dataSchema.getFieldSpecFor(outgoingTimeColumnName)));

    docIdMap = new HashMap<Object, Pair<Long, Object>>();

    invertedIndexMap = new HashMap<String, RealtimeInvertedIndex>();

    for (String dimension : schema.getDimensionNames()) {
      invertedIndexMap.put(dimension, new DimensionInvertertedIndex(dimension));
    }

    for (String metric : schema.getMetricNames()) {
      invertedIndexMap.put(metric, new MetricInvertedIndex(metric));
    }

    invertedIndexMap.put(outgoingTimeColumnName, new TimeInvertedIndex(outgoingTimeColumnName));

    dimensionsSerde = new RealtimeDimensionsSerDe(schema.getDimensionNames(), dataSchema, dictionaryMap);
    metricsSerDe = new RealtimeMetricsSerDe(dataSchema);

  }

  @Override
  public Interval getTimeInterval() {
    DateTime start = timeConverter.getDataTimeFrom(minTimeVal);
    DateTime end = timeConverter.getDataTimeFrom(maxTimeVal);
    return new Interval(start, end);
  }

  public long getMinTime() {
    return minTimeVal;
  }

  public long getMaxTime() {
    return maxTimeVal;
  }

  @Override
  public void index(GenericRow row) {
    numDocsIndexed++;
    // updating dictionary for dimesions only
    // its ok to insert this first
    // since filtering won't return back anything unless a new entry is made in the inverted index
    for (String dimension : dataSchema.getDimensionNames()) {
      dictionaryMap.get(dimension).index(row.getValue(dimension));
      if (!dataSchema.getFieldSpecFor(dimension).isSingleValueField()) {
        Object[] entries = (Object[]) row.getValue(dimension);
        if ((entries != null) && (maxNumberOfMultivaluesMap.get(dimension) < entries.length)) {
          maxNumberOfMultivaluesMap.put(dimension, entries.length);
        }
      }
    }

    // creating ByteBuffer out of dimensions
    ByteBuffer dimBuff = dimensionsSerde.serialize(row);
    // creating ByteBuffer out of metrics
    ByteBuffer metBuff = metricsSerDe.serialize(row);

    // lets rewind both the buffs
    // remove it if there are no sequential access performed ahead
    dimBuff.rewind();
    metBuff.rewind();

    long dimesionHash = HashUtil.compute(dimBuff);
    Object timeValueObj = timeConverter.convert(row.getValue(incomingTimeColumnName));

    long timeValue = -1;
    if (timeValueObj instanceof Integer) {
      timeValue = ((Integer) timeValueObj).longValue();
    } else {
      timeValue = (Long) timeValueObj;
    }
    dictionaryMap.get(outgoingTimeColumnName).index(timeValueObj);
    int timeValueDictId = dictionaryMap.get(outgoingTimeColumnName).indexOf(timeValueObj);

    // update the min max time values
    minTimeVal = Math.min(minTimeVal, timeValue);
    maxTimeVal = Math.max(maxTimeVal, timeValue);

    Pair<Long, Object> dimHashTimePair = Pair.<Long, Object> of(dimesionHash, timeValueObj);

    // checking if the hash already exist
    if (!dimemsionTupleMap.containsKey(dimesionHash)) {
      // create a new tuple
      DimensionTuple dimTuple = new DimensionTuple(dimBuff, dimesionHash);
      // add metrics buffer
      dimTuple.addMetricsbuffFor(timeValueObj, metBuff, dataSchema);

      // add the tuple to the tuple map
      dimemsionTupleMap.put(dimesionHash, dimTuple);

      // generate a new docId and update the docId map
      int docId = docIdGenerator.incrementAndGet();
      docIdMap.put(docId, dimHashTimePair);

      // update invertedIndex since a new docId is generated
      updateInvertedIndex(dimBuff, metBuff, timeValueDictId, docId);
    } else {
      // fetch the existing tuple
      DimensionTuple tuple = dimemsionTupleMap.get(dimesionHash);

      // check if the time value if present in the existing tuple
      if (!tuple.containsTime(timeValueObj)) {
        // generate a new docId and update the docId map
        int docId = docIdGenerator.incrementAndGet();
        docIdMap.put(docId, dimHashTimePair);

        // update inverted index since a new docId is generated
        updateInvertedIndex(dimBuff, metBuff, timeValueDictId, docId);
      }
      tuple.addMetricsbuffFor(timeValueObj, metBuff, dataSchema);
    }
    numSuccessIndexed++;
  }

  /**
  *
  * @param dimBuff
  * @param metBuff
  * @param timeValue
  * @param docId
  *
  */
  public void updateInvertedIndex(ByteBuffer dimBuff, ByteBuffer metBuff, int timeValueDictId, int docId) {

    invertedIndexMap.get(outgoingTimeColumnName).add(timeValueDictId, docId);

    for (String dimension : dataSchema.getDimensionNames()) {
      int[] dicIds = dimensionsSerde.deSerializeAndReturnDicIdsFor(dimension, dimBuff);

      for (int dicId : dicIds) {
        invertedIndexMap.get(dimension).add(new Integer(dicId), docId);
      }
    }

    for (String metric : dataSchema.getMetricNames()) {
      invertedIndexMap.get(metric).add(metricsSerDe.getRawValueFor(metric, metBuff), docId);
    }

    docIdSearchableOffset = docIdGenerator.get() + 1;
  }

  @Override
  public IndexType getIndexType() {
    throw new UnsupportedOperationException("not implemented");
  }

  public void setSegmentName(String segmentId) {
    this.segmentName = segmentId;
  }

  @Override
  public String getSegmentName() {
    return segmentName;
  }

  @Override
  public String getAssociatedDirectory() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public DataSource getDataSource(String columnName) {
    FieldSpec fieldSpec = dataSchema.getFieldSpecFor(columnName);
    DataSource ds = null;
    if (fieldSpec.getFieldType() == FieldType.METRIC) {
      ds =
          new RealtimeColumnDataSource(fieldSpec, null, docIdMap, null, columnName, docIdSearchableOffset, dataSchema,
              dimemsionTupleMap, 0, dimensionsSerde, metricsSerDe);
    }
    if (fieldSpec.getFieldType() == FieldType.DIMENSION) {
      ds =
          new RealtimeColumnDataSource(fieldSpec, dictionaryMap.get(columnName), docIdMap,
              invertedIndexMap.get(columnName), columnName, docIdSearchableOffset, dataSchema, dimemsionTupleMap,
              maxNumberOfMultivaluesMap.get(columnName), dimensionsSerde, metricsSerDe);
    }
    if (fieldSpec.getFieldType() == FieldType.TIME) {
      ds =
          new RealtimeColumnDataSource(fieldSpec, dictionaryMap.get(columnName), docIdMap,
              invertedIndexMap.get(columnName), columnName, docIdSearchableOffset, dataSchema, dimemsionTupleMap, 0,
              dimensionsSerde, metricsSerDe);
    }
    return ds;
  }

  @Override
  public DataSource getDataSource(String columnName, Predicate p) {
    DataSource ds = getDataSource(columnName);
    ds.setPredicate(p);
    return ds;
  }

  @Override
  public String[] getColumnNames() {
    return dataSchema.getColumnNames().toArray(new String[0]);
  }

  @Override
  public void init(Schema dataSchema) {
    throw new UnsupportedOperationException("Not support method: init(Schema) in RealtimeSegmentImpl");
  }

  @Override
  public RecordReader getRecordReader() {
    throw new UnsupportedOperationException("Not support method: getRecordReader() in RealtimeSegmentImpl");
  }

  @Override
  public int getAggregateDocumentCount() {
    return docIdGenerator.get();
  }

  @Override
  public int getRawDocumentCount() {
    return numDocsIndexed;
  }

  public int getSuccessIndexedCount() {
    return numSuccessIndexed;
  }

  public void print() {
    for (String col : dictionaryMap.keySet()) {
      dictionaryMap.get(col).print();
    }
  }

  @Override
  public void destroy() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public GenericRow getRawValueRowAt(int docId) {
    GenericRow row = new GenericRow();
    Map<String, Object> rowValues = new HashMap<String, Object>();

    Pair<Long, Object> dimHashTimePair = docIdMap.get(docId);
    DimensionTuple tuple = dimemsionTupleMap.get(dimHashTimePair.getLeft());
    Object timeValue = dimHashTimePair.getRight();

    ByteBuffer dimBuff = tuple.getDimBuff().duplicate();

    for (String dimension : dataSchema.getDimensionNames()) {
      int[] ret = dimensionsSerde.deSerializeAndReturnDicIdsFor(dimension, dimBuff);
      if (dataSchema.getFieldSpecFor(dimension).isSingleValueField()) {
        rowValues.put(dimension, dictionaryMap.get(dimension).get(ret[0]));
      } else {
        Object[] mV = new Object[ret.length];
        for (int i = 0; i < ret.length; i++) {
          mV[i] = dictionaryMap.get(dimension).get(ret[i]);
        }
        rowValues.put(dimension, mV);
      }
    }
    ByteBuffer metricBuff = tuple.getMetricsBuffForTime(timeValue).duplicate();

    for (String metric : dataSchema.getMetricNames()) {
      rowValues.put(metric, metricsSerDe.getRawValueFor(metric, metricBuff));
    }

    rowValues.put(outgoingTimeColumnName, timeValue);
    row.init(rowValues);

    return row;
  }

  public void setSegmentMetadata(RealtimeSegmentZKMetadata segmentMetadata) {
    _segmentMetadata = new SegmentMetadataImpl(segmentMetadata);
  }

  @Override
  public int getTotalDocs() {
    return docIdSearchableOffset;
  }
}
