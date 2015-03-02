package com.linkedin.pinot.core.realtime.converter;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;


public class RealtimeSegmentRecordReader implements RecordReader {

  private RealtimeSegmentImpl realtimeSegment;
  private Schema dataSchema;
  private List<String> columns;
  int counter = 0;

  public RealtimeSegmentRecordReader(RealtimeSegmentImpl rtSegment, Schema schema) {
    this.realtimeSegment = rtSegment;
    this.dataSchema = schema;
    columns = new ArrayList<String>();
  }

  @Override
  public void init() throws Exception {
    columns.addAll(dataSchema.getDimensionNames());
    columns.addAll(dataSchema.getMetricNames());
    columns.add(dataSchema.getTimeSpec().getOutGoingTimeColumnName());
  }

  @Override
  public void rewind() throws Exception {
    counter = 0;
  }

  @Override
  public boolean hasNext() {
    return counter < realtimeSegment.getNumberOfDocIds();
  }

  @Override
  public Schema getSchema() {
    return dataSchema;
  }

  @Override
  public GenericRow next() {
    GenericRow row = realtimeSegment.getRawValueRowAt(counter);
    counter++;
    return row;
  }

  @Override
  public void close() throws Exception {
    realtimeSegment = null;
  }

}
