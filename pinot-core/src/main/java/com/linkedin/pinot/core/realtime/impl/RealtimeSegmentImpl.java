package com.linkedin.pinot.core.realtime.impl;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.Schema;
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


public class RealtimeSegmentImpl implements RealtimeSegment {
  private final Schema dataSchema;

  private final Map<String, MutableDictionaryReader> dictionaryMap;
  private final Map<Long, DimensionTuple> dimemsionTupleMap;
  private final Map<String, RealtimeInvertedIndex> invertedIndexMap;

  private final TimeConverter timeConverter;
  private AtomicInteger docIdGenerator;
  private String incomingTimeColumnName;
  private String outgoingTimeColumnName;
  private Map<Object, Pair<Long, Long>> docIdMap;
  private Map<String, Integer> maxNumberOfMultivaluesMap;

  private final RealtimeDimensionsSerDe dimensionsSerde;
  private final RealtimeMetricsSerDe metricsSerDe;

  private int docIdSearchableOffset = 0;

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
    docIdMap = new HashMap<Object, Pair<Long, Long>>();

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

  @Override
  public void index(GenericRow row) {
    // updating dictionary for dimesions only
    // its ok to insert this first
    // since filtering won't return back anything unless a new entry is made in the inverted index
    for (String dimension : dataSchema.getDimensionNames()) {
      dictionaryMap.get(dimension).index(row.getValue(dimension));
    }

    // creating ByteBuffer out of dimensions
    IntBuffer dimBuff = dimensionsSerde.serialize(row);
    // creating ByteBuffer out of metrics
    ByteBuffer metBuff = metricsSerDe.serialize(row);

    // lets rewind both the buffs
    // remove it if there are no sequential access performed ahead
    dimBuff.rewind();
    metBuff.rewind();

    long dimesionHash = HashUtil.compute(dimBuff);
    Long timeValue = timeConverter.convert(row.getValue(incomingTimeColumnName));

    // update the min max time values
    if (minTimeVal > timeValue) {
      minTimeVal = timeValue;
    }
    if (maxTimeVal < timeValue) {
      maxTimeVal = timeValue;
    }

    Pair<Long, Long> dimHashTimePair = Pair.<Long, Long> of(dimesionHash, timeValue);

    // checking if the hash already exist
    if (!dimemsionTupleMap.containsKey(dimesionHash)) {
      // create a new tuple
      DimensionTuple dimTuple = new DimensionTuple(dimBuff, dimesionHash);
      // add metrics buffer
      dimTuple.addMetricsbuffFor(timeValue, metBuff, dataSchema);

      // add the tuple to the tuple map
      dimemsionTupleMap.put(dimesionHash, dimTuple);

      // generate a new docId and update the docId map
      int docId = docIdGenerator.incrementAndGet();
      docIdMap.put(docId, dimHashTimePair);

      // update invertedIndex since a new docId is generated
      updateInvertedIndex(dimBuff, metBuff, timeValue, docId);
    } else {
      // fetch the existing tuple
      DimensionTuple tuple = dimemsionTupleMap.get(dimesionHash);

      // check if the time value if present in the existing tuple
      if (!tuple.containsTime(timeValue)) {
        // generate a new docId and update the docId map
        int docId = docIdGenerator.incrementAndGet();
        docIdMap.put(docId, dimHashTimePair);

        // update inverted index since a new docId is generated
        updateInvertedIndex(dimBuff, metBuff, timeValue, docId);
      }
      tuple.addMetricsbuffFor(timeValue, metBuff, dataSchema);
    }
  }

  /**
   *
   * @param dimBuff
   * @param metBuff
   * @param timeValue
   * @param docId
   *
   */
  public void updateInvertedIndex(IntBuffer dimBuff, ByteBuffer metBuff, Long timeValue, int docId) {
    invertedIndexMap.get(outgoingTimeColumnName).add(timeValue, docId);

    for (String dimension : dataSchema.getDimensionNames()) {
      int[] dicIds = dimensionsSerde.deSerializeAndReturnDicIdsFor(dimension, dimBuff);

      for (int dicId : dicIds) {
        invertedIndexMap.get(dimension).add(new Integer(dicId), docId);
      }
    }

    for (String metric : dataSchema.getMetricNames()) {
      invertedIndexMap.get(metric).add(metricsSerDe.getRawValueFor(metric, metBuff), docId);
    }

    docIdSearchableOffset = docIdGenerator.get();
  }

  @Override
  public IndexType getIndexType() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public String getSegmentName() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public String getAssociatedDirectory() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public DataSource getDataSource(String columnName) {
    DataSource ds =
        new RealtimeColumnDataSource(dataSchema.getFieldSpecFor(columnName), dictionaryMap.get(columnName), docIdMap,
            invertedIndexMap.get(columnName), columnName, docIdSearchableOffset, dataSchema, dimemsionTupleMap,
            maxNumberOfMultivaluesMap.get(columnName), dimensionsSerde, metricsSerDe);
    return ds;
  }

  @Override
  public DataSource getDataSource(String columnName, Predicate p) {
    DataSource ds =
        new RealtimeColumnDataSource(dataSchema.getFieldSpecFor(columnName), dictionaryMap.get(columnName), docIdMap,
            invertedIndexMap.get(columnName), columnName, docIdSearchableOffset, dataSchema, dimemsionTupleMap,
            maxNumberOfMultivaluesMap.get(columnName), dimensionsSerde, metricsSerDe);
    ds.setPredicate(p);
    return ds;
  }

  @Override
  public String[] getColumnNames() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void init(Schema dataSchema) {
    // TODO Auto-generated method stub
  }

  @Override
  public RecordReader getRecordReader() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getCurrentDocumentsIndexedCount() {
    return docIdGenerator.get();
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

    Pair<Long, Long> dimHashTimePair = docIdMap.get(docId);
    DimensionTuple tuple = dimemsionTupleMap.get(dimHashTimePair.getLeft());
    Long timeValue = dimHashTimePair.getRight();

    IntBuffer dimBuff = tuple.getDimBuff().duplicate();

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
}
