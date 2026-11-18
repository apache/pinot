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
import java.util.Map;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.OpenStructKeyFlattener;
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
    return blob == null ? null : resolve(blob, key);
  }

  /// Resolves `key` against one document the way [OpenStructKeyFlattener] would have keyed
  /// it at ingest, so `col['o.z']` reads `{"o": {"z": 2}}` from a blob that holds the nested object:
  /// - a key the object carries literally wins at every level (`.` is an ordinary key character);
  /// - otherwise the key is walked through nested objects, taking the first match in document order when two
  ///   synthesized paths collide, as the flattener's first emission does;
  /// - arrays are never indexed into, and a missing, null or scalar intermediate is absent;
  /// - empty key segments are allowed, as the flattener emits them (`{"o": {"": 1}}` keys `o.` to `1`);
  /// - unlike a flattened column, the walk has no depth limit. Native segments store a nested object that was not
  ///   flattened as JSON text, which this does not parse, and a segment with a sparse-key manifest only serves the
  ///   keys it lists, so the walk serves blobs written without flattening: a segment with no manifest, such as one
  ///   produced outside the standard build. A miss costs a scan of each level's field names per document.
  @Nullable
  static JsonNode resolve(JsonNode node, String key) {
    JsonNode literal = node.get(key);
    if (literal != null || key.indexOf(OpenStructKeyFlattener.PATH_SEPARATOR) < 0 || !node.isObject()) {
      return literal;
    }
    for (Map.Entry<String, JsonNode> field : node.properties()) {
      String name = field.getKey();
      JsonNode child = field.getValue();
      if (child.isObject() && key.length() > name.length() && key.startsWith(name)
          && key.charAt(name.length()) == OpenStructKeyFlattener.PATH_SEPARATOR) {
        JsonNode found = resolve(child, key.substring(name.length() + 1));
        if (found != null) {
          return found;
        }
      }
    }
    return null;
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
    return computePresence(key, node -> true);
  }

  /// Full scan for docs where `key` holds a non-null value that `accept` takes. Callers memoize.
  ImmutableRoaringBitmap computePresence(String key, Predicate<JsonNode> accept) {
    // TODO: the opt-in sparse JSON index already encodes this; deriving presence from it needs a
    // handle on the index, which only ImmutableOpenStructDataSource has.
    MutableRoaringBitmap present = new MutableRoaringBitmap();
    try (ForwardIndexReaderContext context = createBlobContext()) {
      for (int docId = 0; docId < _numDocs; docId++) {
        JsonNode val = getValue(docId, key, context);
        if (val != null && !val.isNull() && accept.test(val)) {
          present.add(docId);
        }
      }
    }
    return present.toImmutableRoaringBitmap();
  }
}
