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
package com.linkedin.pinot.core.realtime.converter;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.BaseRecordReader;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO Implement null counting if needed.
public class RealtimeSegmentRecordReader extends BaseRecordReader {

  private final Schema dataSchema;
  private final List<String> columns;
  private final String sortedColumn;

  private RealtimeSegmentImpl realtimeSegment;
  private int counter = 0;
  private Iterator<Integer> docIdIterator = null;

  public RealtimeSegmentRecordReader(RealtimeSegmentImpl rtSegment, Schema schema) {
    super();
    super.initNullCounters(schema);
    this.realtimeSegment = rtSegment;
    this.dataSchema = schema;
    this.columns = new ArrayList<String>();
    this.sortedColumn = null;
    this.docIdIterator = null;
  }

  public RealtimeSegmentRecordReader(RealtimeSegmentImpl rtSegment, Schema schema, String sortedColumn) {
    super();
    super.initNullCounters(schema);
    this.realtimeSegment = rtSegment;
    this.dataSchema = schema;
    columns = new ArrayList<String>();
    this.sortedColumn = sortedColumn;
    this.docIdIterator = realtimeSegment.getSortedDocIdIteratorOnColumn(sortedColumn);
  }

  @Override
  public void init() throws Exception {
    columns.addAll(dataSchema.getDimensionNames());
    columns.addAll(dataSchema.getMetricNames());
    columns.add(dataSchema.getTimeFieldSpec().getOutgoingTimeColumnName());
  }

  @Override
  public void rewind() throws Exception {
    if (docIdIterator == null) {
      counter = 0;
    } else {
      this.docIdIterator = realtimeSegment.getSortedDocIdIteratorOnColumn(this.sortedColumn);
    }
  }

  @Override
  public boolean hasNext() {
    if (docIdIterator == null) {
      return counter < realtimeSegment.getAggregateDocumentCount();
    }
    return docIdIterator.hasNext();
  }

  @Override
  public Schema getSchema() {
    return dataSchema;
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow row) {
    if (docIdIterator == null) {
      row = realtimeSegment.getRawValueRowAt(counter, row);
      counter++;
      return row;
    }
    int docId = docIdIterator.next();
    return realtimeSegment.getRawValueRowAt(docId, row);
  }

  @Override
  public void close() throws Exception {
    realtimeSegment = null;
  }
}
