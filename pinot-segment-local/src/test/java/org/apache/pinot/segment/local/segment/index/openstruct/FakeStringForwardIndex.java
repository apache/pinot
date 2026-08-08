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
package org.apache.pinot.segment.local.segment.index.openstruct;

import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Test stand-in for the sparse column's raw STRING forward index. `null` entries model
/// empty docs (stored as "" with the null vector set, exactly like the splitter writes them).
public class FakeStringForwardIndex implements ForwardIndexReader<ForwardIndexReaderContext> {
  protected final String[] _blobs;

  public FakeStringForwardIndex(String[] blobs) {
    _blobs = blobs;
  }

  public static NullValueVectorReader nullVector(String[] blobs) {
    MutableRoaringBitmap nulls = new MutableRoaringBitmap();
    for (int i = 0; i < blobs.length; i++) {
      if (blobs[i] == null) {
        nulls.add(i);
      }
    }
    ImmutableRoaringBitmap frozen = nulls.toImmutableRoaringBitmap();
    return new NullValueVectorReader() {
      @Override
      public boolean isNull(int docId) {
        return frozen.contains(docId);
      }

      @Override
      public ImmutableRoaringBitmap getNullBitmap() {
        return frozen;
      }
    };
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getStoredType() {
    return DataType.STRING;
  }

  @Override
  public String getString(int docId, ForwardIndexReaderContext context) {
    return _blobs[docId] != null ? _blobs[docId] : "";
  }

  @Override
  public void close() {
  }
}
