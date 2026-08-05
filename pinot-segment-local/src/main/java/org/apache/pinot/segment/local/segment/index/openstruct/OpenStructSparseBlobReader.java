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

import com.fasterxml.jackson.databind.JsonNode;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Shared per-parent sparse blob parser: each read re-parses the doc's blob. Instances are
/// stateless and segment-shared; each caller needs its own [ForwardIndexReaderContext].
/// FastJsonPathExtractor is the planned follow-up for making repeated key reads cheap.
public class OpenStructSparseBlobReader {
  private final ForwardIndexReader<ForwardIndexReaderContext> _blobReader;
  @Nullable
  private final NullValueVectorReader _blobNulls;
  private final int _numDocs;

  @SuppressWarnings("unchecked")
  public OpenStructSparseBlobReader(ForwardIndexReader<?> blobReader, @Nullable NullValueVectorReader blobNulls,
      int numDocs) {
    _blobReader = (ForwardIndexReader<ForwardIndexReaderContext>) blobReader;
    _blobNulls = blobNulls;
    _numDocs = numDocs;
  }

  public int getNumDocs() {
    return _numDocs;
  }

  @Nullable
  public ForwardIndexReaderContext createBlobContext() {
    return _blobReader.createContext();
  }

  @Nullable
  public JsonNode getValue(int docId, String key, @Nullable ForwardIndexReaderContext context) {
    // TODO: an N-key projection parses each doc N times. FastJsonPathExtractor fixes this in one pass,
    // but only across a batch of paths — this single-key signature has to change first.
    JsonNode blob = parseBlob(docId, context);
    return blob == null ? null : blob.get(key);
  }

  @Nullable
  private JsonNode parseBlob(int docId, @Nullable ForwardIndexReaderContext context) {
    if (_blobNulls != null && _blobNulls.isNull(docId)) {
      return null;
    }
    String json = _blobReader.getString(docId, context);
    if (json.isEmpty()) {
      return null;
    }
    try {
      return JsonUtils.stringToJsonNode(json);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to parse OPEN_STRUCT sparse blob at doc " + docId, e);
    }
  }

  /// Full scan for docs where `key` is present (explicit JSON null = absent). Callers memoize.
  public ImmutableRoaringBitmap computePresence(String key) {
    // TODO: the opt-in sparse JSON index already encodes this; deriving presence from it needs a
    // handle on the index, which only ImmutableOpenStructDataSource has.
    MutableRoaringBitmap present = new MutableRoaringBitmap();
    try (ForwardIndexReaderContext context = createBlobContext()) {
      for (int docId = 0; docId < _numDocs; docId++) {
        JsonNode val = getValue(docId, key, context);
        if (val != null && !val.isNull()) {
          present.add(docId);
        }
      }
    }
    return present.toImmutableRoaringBitmap();
  }
}
