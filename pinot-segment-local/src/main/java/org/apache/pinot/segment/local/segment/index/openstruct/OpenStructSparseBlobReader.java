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
import com.fasterxml.jackson.databind.node.MissingNode;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Shared per-parent sparse blob parser with a ThreadLocal LRU cache so multi-key projections
/// parse each doc's blob once. Thread-safe: cache is ThreadLocal, forward reader is segment-shared.
public class OpenStructSparseBlobReader {
  static final int PARSE_CACHE_SIZE = 10_000;

  private final ForwardIndexReader<ForwardIndexReaderContext> _blobReader;
  @Nullable
  private final NullValueVectorReader _blobNulls;
  private final int _numDocs;

  private final ThreadLocal<LinkedHashMap<Integer, JsonNode>> _parseCache =
      ThreadLocal.withInitial(() -> new LinkedHashMap<>(1024, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Integer, JsonNode> eldest) {
          return size() > PARSE_CACHE_SIZE;
        }
      });

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
    JsonNode blob = getBlob(docId, context);
    return blob == null ? null : blob.get(key);
  }

  @Nullable
  private JsonNode getBlob(int docId, @Nullable ForwardIndexReaderContext context) {
    LinkedHashMap<Integer, JsonNode> cache = _parseCache.get();
    JsonNode cached = cache.get(docId);
    if (cached != null) {
      return cached.isMissingNode() ? null : cached;
    }
    JsonNode parsed = parseBlob(docId, context);
    cache.put(docId, parsed == null ? MissingNode.getInstance() : parsed);
    return parsed;
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
