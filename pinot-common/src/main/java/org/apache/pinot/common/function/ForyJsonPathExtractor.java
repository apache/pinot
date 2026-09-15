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
package org.apache.pinot.common.function;

import com.fasterxml.jackson.core.StreamReadConstraints;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.fory.json.ForyJson;
import org.apache.fory.json.codec.JsonValueCodec;
import org.apache.fory.json.reader.JsonReader;
import org.apache.fory.json.reader.Latin1JsonReader;
import org.apache.fory.json.reader.Utf16JsonReader;
import org.apache.fory.json.reader.Utf8JsonReader;
import org.apache.fory.json.writer.StringJsonWriter;
import org.apache.fory.json.writer.Utf8JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Shared Fory JSON parser for opt-in JSON-path implementations.
///
/// Streaming extraction uses one bounded parser pool shared by all worker threads. Initialization or runtime linkage
/// failures permanently disable the optional path, allowing callers to fall back to Jackson/Jayway. Jackson's
/// document, token, nesting, field-name, string, and number constraints are enforced while walking the document.
/// The Fory runtime is an optional dependency; applications must add `org.apache.fory:fory-json:1.6.0` (and its
/// transitive `fory-core` dependency) to the application classpath to enable this experimental path.
public final class ForyJsonPathExtractor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ForyJsonPathExtractor.class);
  private static final StreamReadConstraints JACKSON_CONSTRAINTS = StreamReadConstraints.defaults();
  private static final ThreadLocal<PathContext> PATH_CONTEXT = ThreadLocal.withInitial(PathContext::new);
  private static final AtomicBoolean UNAVAILABLE_WARNING_LOGGED = new AtomicBoolean();
  private static final Object FALLBACK_REQUIRED = new Object();

  private ForyJsonPathExtractor() {
  }

  private static final class Holder {
    @Nullable
    private static final ForyJson STREAMING_PARSER = buildStreamingParser();
    private static volatile boolean _streamingAvailable = STREAMING_PARSER != null;

    private Holder() {
    }

    @Nullable
    private static ForyJson buildStreamingParser() {
      try {
        return ForyJson.builder().withCodegen(false).withAsyncCompilation(false)
            .maxDepth(JACKSON_CONSTRAINTS.getMaxNestingDepth())
            .registerCodec(PathResult.class, PathCodec.INSTANCE).build();
      } catch (RuntimeException | LinkageError e) {
        logUnavailable(e);
        return null;
      }
    }
  }

  /// Returns whether the optional Fory runtime initialized successfully.
  public static boolean isAvailable() {
    return Holder._streamingAvailable;
  }

  /// Returns whether an extracted value requires the reference parser for exact container/coercion semantics.
  public static boolean isFallbackRequired(@Nullable Object value) {
    return value == FALLBACK_REQUIRED;
  }

  /// Extracts a simple path with Fory's streaming reader without materializing the complete JSON tree.
  ///
  /// Unrelated values are still fully consumed so malformed input and duplicate-key last-wins behavior match the
  /// reference parser. Jackson's configured limits are checked while scanning. Callers should retry with the
  /// reference parser when this method throws or [#isFallbackRequired(Object)] returns `true`.
  @Nullable
  public static Object extract(String json, SimpleJsonPath path) {
    if (JACKSON_CONSTRAINTS.hasMaxDocumentLength()
        && json.length() > JACKSON_CONSTRAINTS.getMaxDocumentLength()) {
      throw new IllegalArgumentException("JSON document requires Jackson constraint validation");
    }
    if (!Holder._streamingAvailable) {
      throw new IllegalStateException("Fory JSON is unavailable");
    }
    ForyJson parser = Holder.STREAMING_PARSER;
    if (parser == null) {
      throw new IllegalStateException("Fory JSON is unavailable");
    }
    PathContext context = PATH_CONTEXT.get();
    if (context._active) {
      throw new IllegalStateException("Fory JSON path extraction is not reentrant");
    }
    context._active = true;
    context._path = path;
    context._result = null;
    context._tokenCount = 0;
    try {
      parser.fromJson(json, PathResult.class);
      return context._result;
    } catch (LinkageError e) {
      disable();
      logUnavailable(e);
      throw new IllegalStateException("Fory JSON became unavailable", e);
    } finally {
      context._path = null;
      context._result = null;
      context._tokenCount = 0;
      context._active = false;
    }
  }

  private static void disable() {
    Holder._streamingAvailable = false;
    PATH_CONTEXT.remove();
  }

  private static void logUnavailable(Throwable cause) {
    if (UNAVAILABLE_WARNING_LOGGED.compareAndSet(false, true)) {
      LOGGER.warn("Experimental Fory JSON support is unavailable; falling back to Jackson/Jayway", cause);
    }
  }

  private static Object readPath(JsonReader reader, SimpleJsonPath path, int depth, PathContext context) {
    String key = path.getKey(depth);
    if (key != null) {
      return readObjectPath(reader, path, depth, key, context);
    }
    return readArrayPath(reader, path, depth, path.getIndex(depth), context);
  }

  @Nullable
  private static Object readObjectPath(JsonReader reader, SimpleJsonPath path, int depth, String expectedKey,
      PathContext context) {
    if (reader.peekToken() != '{') {
      skipValue(reader, context);
      return null;
    }
    reader.enterDepth();
    try {
      countToken(context);
      reader.expect('{');
      if (reader.consume('}')) {
        countToken(context);
        return null;
      }
      Object result = null;
      boolean more;
      do {
        countToken(context);
        String fieldName = reader.readFieldName();
        if (fieldName.length() > JACKSON_CONSTRAINTS.getMaxNameLength()) {
          throw new IllegalArgumentException("JSON field name exceeds Jackson's configured limit");
        }
        reader.expect(':');
        if (expectedKey.equals(fieldName)) {
          result = depth + 1 == path.length() ? readScalar(reader, context)
              : readPath(reader, path, depth + 1, context);
        } else {
          skipValue(reader, context);
        }
        more = reader.consumeCommaOrEndObject();
      } while (more);
      countToken(context);
      return result;
    } finally {
      reader.exitDepth();
    }
  }

  @Nullable
  private static Object readArrayPath(JsonReader reader, SimpleJsonPath path, int depth, int expectedIndex,
      PathContext context) {
    if (reader.peekToken() != '[') {
      skipValue(reader, context);
      return null;
    }
    reader.enterDepth();
    try {
      countToken(context);
      reader.expect('[');
      if (reader.consume(']')) {
        countToken(context);
        return null;
      }
      Object result = null;
      int index = 0;
      boolean more;
      do {
        if (index == expectedIndex) {
          result = depth + 1 == path.length() ? readScalar(reader, context)
              : readPath(reader, path, depth + 1, context);
        } else {
          skipValue(reader, context);
        }
        index++;
        more = reader.consumeCommaOrEndArray();
      } while (more);
      countToken(context);
      return result;
    } finally {
      reader.exitDepth();
    }
  }

  @Nullable
  private static Object readScalar(JsonReader reader, PathContext context) {
    char token = reader.peekToken();
    if (token == '"') {
      countToken(context);
      String value = reader.readString();
      if (value.length() > JACKSON_CONSTRAINTS.getMaxStringLength()) {
        throw new IllegalArgumentException("JSON string exceeds Jackson's configured limit");
      }
      return value;
    }
    if (token == 't' || token == 'f') {
      countToken(context);
      return reader.readBoolean();
    }
    if (token == 'n') {
      countToken(context);
      reader.readNull();
      return null;
    }
    if (token == '{' || token == '[') {
      // Query scalar coercion has observable error/default behavior for containers. Fully consume the value to keep
      // malformed-tail and duplicate-key semantics, then signal a stack-trace-free retry through Jayway.
      skipValue(reader, context);
      return FALLBACK_REQUIRED;
    }
    countToken(context);
    int start = reader.position();
    Number value = reader.readNumber();
    if (reader.position() - start > JACKSON_CONSTRAINTS.getMaxNumberLength()) {
      throw new IllegalArgumentException("JSON number exceeds Jackson's configured limit");
    }
    return value;
  }

  private static void skipValue(JsonReader reader, PathContext context) {
    char token = reader.peekToken();
    if (token == '{') {
      skipObject(reader, context);
      return;
    }
    if (token == '[') {
      skipArray(reader, context);
      return;
    }
    if (token == '"') {
      countToken(context);
      // Fory 1.6's skipValue() computes an FNV hash over every character. Its string decoder uses packed scans and
      // is substantially faster even when the decoded value is discarded. An upstream fast-skip API could remove
      // this temporary allocation in a future Fory version.
      String value = reader.readString();
      if (value.length() > JACKSON_CONSTRAINTS.getMaxStringLength()) {
        throw new IllegalArgumentException("JSON string exceeds Jackson's configured limit");
      }
      return;
    }
    countToken(context);
    int start = reader.position();
    reader.skipValue();
    int rawLength = reader.position() - start;
    if (token != 't' && token != 'f' && token != 'n'
        && rawLength > JACKSON_CONSTRAINTS.getMaxNumberLength()) {
      throw new IllegalArgumentException("JSON number exceeds Jackson's configured limit");
    }
  }

  private static void skipObject(JsonReader reader, PathContext context) {
    reader.enterDepth();
    try {
      countToken(context);
      reader.expect('{');
      if (reader.consume('}')) {
        countToken(context);
        return;
      }
      boolean more;
      do {
        countToken(context);
        String fieldName = reader.readFieldName();
        if (fieldName.length() > JACKSON_CONSTRAINTS.getMaxNameLength()) {
          throw new IllegalArgumentException("JSON field name exceeds Jackson's configured limit");
        }
        reader.expect(':');
        skipValue(reader, context);
        more = reader.consumeCommaOrEndObject();
      } while (more);
      countToken(context);
    } finally {
      reader.exitDepth();
    }
  }

  private static void skipArray(JsonReader reader, PathContext context) {
    reader.enterDepth();
    try {
      countToken(context);
      reader.expect('[');
      if (reader.consume(']')) {
        countToken(context);
        return;
      }
      boolean more;
      do {
        skipValue(reader, context);
        more = reader.consumeCommaOrEndArray();
      } while (more);
      countToken(context);
    } finally {
      reader.exitDepth();
    }
  }

  private static final class PathContext {
    private final PathResult _marker = new PathResult();
    private boolean _active;
    private long _tokenCount;
    @Nullable
    private SimpleJsonPath _path;
    @Nullable
    private Object _result;
  }

  private static final class PathResult {
  }

  private static final class PathCodec implements JsonValueCodec<PathResult> {
    private static final PathCodec INSTANCE = new PathCodec();

    @Override
    public PathResult readLatin1(Latin1JsonReader reader) {
      return read(reader);
    }

    @Override
    public PathResult readUtf16(Utf16JsonReader reader) {
      return read(reader);
    }

    @Override
    public PathResult readUtf8(Utf8JsonReader reader) {
      return read(reader);
    }

    private static PathResult read(JsonReader reader) {
      PathContext context = PATH_CONTEXT.get();
      SimpleJsonPath path = context._path;
      if (!context._active || path == null) {
        throw new IllegalStateException("Missing JSON path extraction context");
      }
      context._result = readPath(reader, path, 0, context);
      return context._marker;
    }

    @Override
    public void writeString(StringJsonWriter writer, PathResult value) {
      throw new UnsupportedOperationException("PathResult is read-only");
    }

    @Override
    public void writeUtf8(Utf8JsonWriter writer, PathResult value) {
      throw new UnsupportedOperationException("PathResult is read-only");
    }
  }

  private static void countToken(PathContext context) {
    if (JACKSON_CONSTRAINTS.hasMaxTokenCount()
        && ++context._tokenCount > JACKSON_CONSTRAINTS.getMaxTokenCount()) {
      throw new IllegalArgumentException("JSON token count exceeds Jackson's configured limit");
    }
  }
}
