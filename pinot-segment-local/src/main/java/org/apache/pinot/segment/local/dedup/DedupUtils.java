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
package org.apache.pinot.segment.local.dedup;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PrimaryKeyReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public class DedupUtils {
  private DedupUtils() {
  }

  public static class DedupRecordInfoReader implements Closeable {
    private final PrimaryKeyReader _primaryKeyReader;
    private final PinotSegmentColumnReader _dedupTimeColumnReader;

    public DedupRecordInfoReader(IndexSegment segment, List<String> primaryKeyColumns,
        @Nullable String dedupTimeColumn) {
      _primaryKeyReader = new PrimaryKeyReader(segment, primaryKeyColumns);
      if (dedupTimeColumn != null) {
        _dedupTimeColumnReader = new PinotSegmentColumnReader(segment, dedupTimeColumn);
      } else {
        _dedupTimeColumnReader = null;
      }
    }

    @VisibleForTesting
    public DedupRecordInfoReader(PrimaryKeyReader primaryKeyReader,
        @Nullable PinotSegmentColumnReader dedupTimeColumnReader) {
      _primaryKeyReader = primaryKeyReader;
      _dedupTimeColumnReader = dedupTimeColumnReader;
    }

    public DedupRecordInfo getDedupRecordInfo(int docId) {
      PrimaryKey primaryKey = _primaryKeyReader.getPrimaryKey(docId);
      double dedupTime =
          (_dedupTimeColumnReader != null) ? ((Number) _dedupTimeColumnReader.getValue(docId)).doubleValue()
              : Double.MIN_VALUE;
      return new DedupRecordInfo(primaryKey, dedupTime);
    }

    @Override
    public void close()
        throws IOException {
      _primaryKeyReader.close();
      if (_dedupTimeColumnReader != null) {
        _dedupTimeColumnReader.close();
      }
    }
  }

  /**
   * Returns an iterator of {@link DedupRecordInfo} for all the documents from the segment.
   */
  public static Iterator<DedupRecordInfo> getDedupRecordInfoIterator(DedupRecordInfoReader dedupRecordInfoReader,
      int numDocs) {
    return new Iterator<DedupRecordInfo>() {
      private int _docId = 0;

      @Override
      public boolean hasNext() {
        return _docId < numDocs;
      }

      @Override
      public DedupRecordInfo next() {
        return dedupRecordInfoReader.getDedupRecordInfo(_docId++);
      }
    };
  }
}
