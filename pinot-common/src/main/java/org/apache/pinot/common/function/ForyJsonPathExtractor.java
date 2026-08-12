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
/// Streaming extraction uses one single-state parser per worker thread to avoid shared-pool contention. Initialization
/// or runtime linkage failures permanently disable the optional path, allowing callers to fall back to
/// Jackson/Jayway. Jackson's default scalar token limits are enforced while walking the document, and inputs outside
/// Fory's safe nesting depth fall back to the reference parser.
public final class ForyJsonPathExtractor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ForyJsonPathExtractor.class);
  private static final StreamReadConstraints JACKSON_CONSTRAINTS = StreamReadConstraints.defaults();
  private static final ThreadLocal<PathContext> PATH_CONTEXT = ThreadLocal.withInitial(PathContext::new);
  private static final AtomicBoolean UNAVAILABLE_WARNING_LOGGED = new AtomicBoolean();

  private ForyJsonPathExtractor() {
  }

  private static final class Holder {
    private static volatile boolean _streamingAvailable;
    private static final ThreadLocal<ForyJson> STREAMING_PARSER = ThreadLocal.withInitial(() -> {
      if (!_streamingAvailable) {
        throw new IllegalStateException("Fory JSON is unavailable");
      }
      ForyJson parser = buildStreamingParser();
      if (parser == null) {
        _streamingAvailable = false;
        throw new IllegalStateException("Fory JSON is unavailable");
      }
      return parser;
    });

    static {
      ForyJson parser = buildStreamingParser();
      _streamingAvailable = parser != null;
      if (parser != null) {
        STREAMING_PARSER.set(parser);
      }
    }

    private Holder() {
    }

    @Nullable
    private static ForyJson buildStreamingParser() {
      try {
        return ForyJson.builder().withCodegen(false).withAsyncCompilation(false).withConcurrencyLevel(1)
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

  /// Extracts a simple path with Fory's streaming reader without materializing the complete JSON tree.
  ///
  /// Unrelated values are still fully consumed so malformed input and duplicate-key last-wins behavior match the
  /// reference parser. Jackson's nesting, field-name, string, and number limits are checked while scanning. Callers
  /// should retry with the reference parser when this method throws.
  @Nullable
  public static Object extract(String json, SimpleJsonPath path) {
    if ((JACKSON_CONSTRAINTS.hasMaxDocumentLength()
        && json.length() > JACKSON_CONSTRAINTS.getMaxDocumentLength())
        || (JACKSON_CONSTRAINTS.hasMaxTokenCount() && requiresJacksonFallback(json))) {
      throw new IllegalArgumentException("JSON document requires Jackson constraint validation");
    }
    if (!Holder._streamingAvailable) {
      throw new IllegalStateException("Fory JSON is unavailable");
    }
    ForyJson parser = Holder.STREAMING_PARSER.get();
    PathContext context = PATH_CONTEXT.get();
    if (context._active) {
      throw new IllegalStateException("Fory JSON path extraction is not reentrant");
    }
    context._active = true;
    context._path = path;
    context._result = null;
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
      context._active = false;
    }
  }

  private static void disable() {
    Holder._streamingAvailable = false;
    Holder.STREAMING_PARSER.remove();
    PATH_CONTEXT.remove();
  }

  private static void logUnavailable(Throwable cause) {
    if (UNAVAILABLE_WARNING_LOGGED.compareAndSet(false, true)) {
      LOGGER.warn("Experimental Fory JSON support is unavailable; falling back to Jackson/Jayway", cause);
    }
  }

  private static Object readPath(JsonReader reader, SimpleJsonPath path, int depth) {
    String key = path.getKey(depth);
    if (key != null) {
      return readObjectPath(reader, path, depth, key);
    }
    return readArrayPath(reader, path, depth, path.getIndex(depth));
  }

  @Nullable
  private static Object readObjectPath(JsonReader reader, SimpleJsonPath path, int depth, String expectedKey) {
    if (reader.peekToken() != '{') {
      skipValue(reader);
      return null;
    }
    reader.enterDepth();
    try {
      reader.expect('{');
      if (reader.consume('}')) {
        return null;
      }
      Object result = null;
      boolean more;
      do {
        String fieldName = reader.readFieldName();
        if (fieldName.length() > JACKSON_CONSTRAINTS.getMaxNameLength()) {
          throw new IllegalArgumentException("JSON field name exceeds Jackson's configured limit");
        }
        reader.expect(':');
        if (expectedKey.equals(fieldName)) {
          result = depth + 1 == path.length() ? readScalar(reader) : readPath(reader, path, depth + 1);
        } else {
          skipValue(reader);
        }
        more = reader.consumeCommaOrEndObject();
      } while (more);
      return result;
    } finally {
      reader.exitDepth();
    }
  }

  @Nullable
  private static Object readArrayPath(JsonReader reader, SimpleJsonPath path, int depth, int expectedIndex) {
    if (reader.peekToken() != '[') {
      skipValue(reader);
      return null;
    }
    reader.enterDepth();
    try {
      reader.expect('[');
      if (reader.consume(']')) {
        return null;
      }
      Object result = null;
      int index = 0;
      boolean more;
      do {
        if (index == expectedIndex) {
          result = depth + 1 == path.length() ? readScalar(reader) : readPath(reader, path, depth + 1);
        } else {
          skipValue(reader);
        }
        index++;
        more = reader.consumeCommaOrEndArray();
      } while (more);
      return result;
    } finally {
      reader.exitDepth();
    }
  }

  @Nullable
  private static Object readScalar(JsonReader reader) {
    char token = reader.peekToken();
    if (token == '"') {
      String value = reader.readString();
      if (value.length() > JACKSON_CONSTRAINTS.getMaxStringLength()) {
        throw new IllegalArgumentException("JSON string exceeds Jackson's configured limit");
      }
      return value;
    }
    if (token == 't' || token == 'f') {
      return reader.readBoolean();
    }
    if (token == 'n') {
      reader.readNull();
      return null;
    }
    if (token == '{' || token == '[') {
      // Query scalar coercion has observable error/default behavior for containers. Let Jayway produce the exact
      // reference value rather than materializing a Fory container on this uncommon path.
      throw new IllegalArgumentException("Container result requires reference JSON parsing");
    }
    int start = reader.position();
    Number value = reader.readNumber();
    if (reader.position() - start > JACKSON_CONSTRAINTS.getMaxNumberLength()) {
      throw new IllegalArgumentException("JSON number exceeds Jackson's configured limit");
    }
    return value;
  }

  private static void skipValue(JsonReader reader) {
    char token = reader.peekToken();
    if (token == '{') {
      skipObject(reader);
      return;
    }
    if (token == '[') {
      skipArray(reader);
      return;
    }
    if (token == '"') {
      // Fory 1.6's skipValue() computes an FNV hash over every character. Its string decoder uses packed scans and
      // is substantially faster even when the decoded value is discarded. An upstream fast-skip API could remove
      // this temporary allocation in a future Fory version.
      String value = reader.readString();
      if (value.length() > JACKSON_CONSTRAINTS.getMaxStringLength()) {
        throw new IllegalArgumentException("JSON string exceeds Jackson's configured limit");
      }
      return;
    }
    int start = reader.position();
    reader.skipValue();
    int rawLength = reader.position() - start;
    if (token != 't' && token != 'f' && token != 'n'
        && rawLength > JACKSON_CONSTRAINTS.getMaxNumberLength()) {
      throw new IllegalArgumentException("JSON number exceeds Jackson's configured limit");
    }
  }

  private static void skipObject(JsonReader reader) {
    reader.enterDepth();
    try {
      reader.expect('{');
      if (reader.consume('}')) {
        return;
      }
      boolean more;
      do {
        String fieldName = reader.readFieldName();
        if (fieldName.length() > JACKSON_CONSTRAINTS.getMaxNameLength()) {
          throw new IllegalArgumentException("JSON field name exceeds Jackson's configured limit");
        }
        reader.expect(':');
        skipValue(reader);
        more = reader.consumeCommaOrEndObject();
      } while (more);
    } finally {
      reader.exitDepth();
    }
  }

  private static void skipArray(JsonReader reader) {
    reader.enterDepth();
    try {
      reader.expect('[');
      if (reader.consume(']')) {
        return;
      }
      boolean more;
      do {
        skipValue(reader);
        more = reader.consumeCommaOrEndArray();
      } while (more);
    } finally {
      reader.exitDepth();
    }
  }

  private static final class PathContext {
    private final PathResult _marker = new PathResult();
    private boolean _active;
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
      context._result = readPath(reader, path, 0);
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

  private static boolean requiresJacksonFallback(String json) {
    int length = json.length();
    if (JACKSON_CONSTRAINTS.hasMaxDocumentLength()
        && length > JACKSON_CONSTRAINTS.getMaxDocumentLength()) {
      return true;
    }

    int maxNumberLength = JACKSON_CONSTRAINTS.getMaxNumberLength();
    int maxStringLength = JACKSON_CONSTRAINTS.getMaxStringLength();
    int maxNameLength = JACKSON_CONSTRAINTS.getMaxNameLength();
    int maxNestingDepth = JACKSON_CONSTRAINTS.getMaxNestingDepth();
    long maxTokenCount = JACKSON_CONSTRAINTS.getMaxTokenCount();
    boolean countTokens = JACKSON_CONSTRAINTS.hasMaxTokenCount();
    int depth = 0;
    long tokenCount = 0;

    int offset = 0;
    while (offset < length) {
      char current = json.charAt(offset);
      if (current == '"') {
        int start = offset++;
        while (offset < length && json.charAt(offset) != '"') {
          if (json.charAt(offset) == '\\') {
            offset++;
          }
          offset++;
        }
        if (offset >= length) {
          return true;
        }
        int rawLength = offset - start - 1;
        int next = offset + 1;
        while (next < length && Character.isWhitespace(json.charAt(next))) {
          next++;
        }
        boolean fieldName = next < length && json.charAt(next) == ':';
        if (rawLength > (fieldName ? maxNameLength : maxStringLength)) {
          return true;
        }
        tokenCount++;
        offset++;
      } else if (current == '{' || current == '[') {
        if (++depth > maxNestingDepth) {
          return true;
        }
        tokenCount++;
        offset++;
      } else if (current == '}' || current == ']') {
        depth--;
        tokenCount++;
        offset++;
      } else if (current == '-' || current >= '0' && current <= '9') {
        int start = offset++;
        while (offset < length && isNumberCharacter(json.charAt(offset))) {
          offset++;
        }
        if (offset - start > maxNumberLength) {
          return true;
        }
        tokenCount++;
      } else if (current == 't' || current == 'f' || current == 'n') {
        tokenCount++;
        offset++;
      } else {
        offset++;
      }
      if (countTokens && tokenCount > maxTokenCount) {
        return true;
      }
    }
    return false;
  }

  private static boolean isNumberCharacter(char value) {
    return value >= '0' && value <= '9' || value == '-' || value == '+' || value == '.' || value == 'e'
        || value == 'E';
  }
}
