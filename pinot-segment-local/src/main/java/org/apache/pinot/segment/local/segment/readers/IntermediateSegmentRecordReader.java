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
package org.apache.pinot.segment.local.segment.readers;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.indexsegment.mutable.IntermediateSegment;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


public class IntermediateSegmentRecordReader implements RecordReader {
  private final IntermediateSegment _intermediateSegment;
  private final int _numDocs;

  private int _nextDocId = 0;

  public IntermediateSegmentRecordReader(IntermediateSegment intermediateSegment) {
    _intermediateSegment = intermediateSegment;
    _numDocs = intermediateSegment.getNumDocsIndexed();
    // TODO: support sorted column like _sortedDocIdIterationOrder in {@link RealtimeSegmentRecordReader}
  }

  @Override
  public void init(File dataFile, Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
  }

  @Override
  public boolean hasNext() {
    return _nextDocId < _numDocs;
  }

  @Override
  public GenericRow next() throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) throws IOException {
    return _intermediateSegment.getRecord(_nextDocId++, reuse);
  }

  @Override
  public void rewind() throws IOException {
    _nextDocId = 0;
  }

  @Override
  public void close() throws IOException {
    _intermediateSegment.destroy();
  }

  public IntermediateSegment getIntermediateSegment() {
    return _intermediateSegment;
  }
}
