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

import java.io.IOException;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;
import org.apache.pinot.spi.data.readers.ColumnarDataSource;
import org.roaringbitmap.RoaringBitmap;


/**
 * ColumnarDataSource implementation that wraps a Pinot segment.
 * Provides columnar access to segment data via ColumnReaderFactory.
 */
public class PinotSegmentColumnarDataSource implements ColumnarDataSource {

  private final IndexSegment _indexSegment;
  private final int _totalDocs;
  private final RoaringBitmap _validDocIds;

  /**
   * @param indexSegment Source segment to read from
   *
   */
  public PinotSegmentColumnarDataSource(IndexSegment indexSegment, RoaringBitmap validDocIds) {
    _indexSegment = indexSegment;
    _totalDocs = indexSegment.getSegmentMetadata().getTotalDocs();
    _validDocIds = validDocIds;
  }

  @Override
  public int getTotalDocs() {
    return _totalDocs;
  }

  @Override
  public ColumnReaderFactory createColumnReaderFactory() {
    return new PinotSegmentColumnReaderFactory(_indexSegment, true, false);
  }

  @Override
  public RoaringBitmap getValidDocIds() {
    return _validDocIds;
  }

  @Override
  public void close()
      throws IOException {
    _indexSegment.destroy();
  }

  @Override
  public String toString() {
    return "PinotSegmentColumnarDataSource{segment=" + _indexSegment.getSegmentName() + "}";
  }
}
