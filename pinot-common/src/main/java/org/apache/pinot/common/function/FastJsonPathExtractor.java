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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.jayway.jsonpath.InvalidJsonException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/// Resolves one or more [SimpleJsonPath]s against a JSON document in a single forward pass of a Jackson
/// [JsonParser], materializing only the addressed values. Subtrees that no path descends into are consumed
/// with `skipChildren()`, so no intermediate `Map` / `List` is ever built for them. This replaces
/// `JsonPath.parse(json).read(path)`, which builds a full Jackson DOM of the document and only then walks to
/// the field.
///
/// **Semantics.** The results are byte-for-byte identical to Jayway configured as Pinot configures it
/// (`JacksonJsonProvider` + `JacksonMappingProvider` + `Option.SUPPRESS_EXCEPTIONS`), including:
/// - a missing key, an out-of-range index, indexing a non-array, descending into a scalar, and an explicit
///   JSON `null` all resolve to `null`;
/// - duplicate object keys resolve to the **last** occurrence, and a later occurrence discards whatever an
///   earlier one resolved to, at every depth;
/// - a container leaf is returned as a `LinkedHashMap` / `ArrayList` with the same element types;
/// - numbers become `Integer` / `Long` / `BigInteger` by magnitude, and `Double` (or `BigDecimal` when
///   `useBigDecimal` is set) for floating-point literals;
/// - a document that is malformed anywhere inside its root value raises [InvalidJsonException], even when the
///   addressed field was already seen; content *after* the root value is ignored, exactly as
///   `ObjectMapper.readValue` ignores it (`FAIL_ON_TRAILING_TOKENS` is off by default).
///
/// **The one place it deliberately differs.** Jayway builds a DOM of the *whole* document, so it materializes
/// and validates every value in it - including fields the path never addresses. This extractor skips those
/// subtrees, so a value that Jackson can lex but cannot *materialize* raises in Jayway and not here. Two such
/// values exist, and both only matter when they sit **outside** the addressed path:
/// 1. a string longer than `StreamReadConstraints` `maxStringLength` (20,000,000 chars);
/// 2. when `useBigDecimal` is set, a float literal whose exponent overflows an `int` (e.g. `1e999999999999`),
///    which `BigDecimal` cannot represent.
/// Jayway rejects such a document outright; this extractor returns the requested value. When the offending
/// value *is* the addressed one, both raise, so the results still agree.
///
/// This is inherent to the optimization - not materializing what was not asked for is precisely what makes it
/// fast, and matching Jayway here would mean materializing every string in every document. The extractor never
/// returns a *different value* for the addressed path; it only declines to fail on values the caller did not
/// ask about, and it is the safer of the two (it never allocates the 20 MB string or the pathological
/// `BigDecimal`).
///
/// **Early exit.** When the caller passes `earlyExit`, the pass stops as soon as every requested path has
/// resolved to a non-null value. That is much faster when the target fields appear early in the document, at
/// the cost of two behavior changes, since the tail is never read:
/// 1. duplicate keys resolve to the **first** occurrence rather than the last;
/// 2. a document malformed strictly after the last addressed field yields the extracted value instead of
///    raising [InvalidJsonException].
/// Both inputs are undefined (RFC 8259) or corrupt, and the switch is off by default.
///
/// **Applicability.** Callers must first check [#canExtract]: the document's first non-whitespace character
/// has to be `{` or `[`. Bare scalar documents, `null`, empty / whitespace-only input, a leading byte-order
/// mark and plain text all take the Jayway path, which reproduces its exact exceptions for them.
///
/// Stateless and thread-safe; the shared [JsonFactory] and [ObjectMapper]s are safe for concurrent use.
public final class FastJsonPathExtractor {
  private FastJsonPathExtractor() {
  }

  /// The live-path set is tracked as a bitmask, so a single call cannot resolve more paths than a `long` has
  /// bits. Callers with more paths must batch them.
  public static final int MAX_PATHS = Long.SIZE;

  /// Deliberately the plain, unconfigured factory and mappers that Jayway's `JacksonJsonProvider` builds, so
  /// that stream-read constraints (nesting depth, string length) and number handling match exactly.
  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectMapper MAPPER_WITH_BIG_DECIMAL =
      new ObjectMapper().configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

  /// Returns `true` when `json`'s first non-whitespace character starts a JSON object or array, which is the
  /// only shape this extractor handles. Everything else must go through Jayway.
  public static boolean canExtract(@Nullable String json) {
    if (json == null) {
      return false;
    }
    for (int i = 0, n = json.length(); i < n; i++) {
      char c = json.charAt(i);
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
        continue;
      }
      return c == '{' || c == '[';
    }
    return false;
  }

  /// UTF-8 overload of [#canExtract(String)]. A byte-order mark is rejected, matching the `String` overload.
  public static boolean canExtract(@Nullable byte[] json) {
    if (json == null) {
      return false;
    }
    for (byte b : json) {
      if (b == ' ' || b == '\t' || b == '\n' || b == '\r') {
        continue;
      }
      return b == '{' || b == '[';
    }
    return false;
  }

  /// Resolves a single `path` against `json`, returning the addressed value or `null` when it resolves to a
  /// missing key, an out-of-range index, a type mismatch, or an explicit JSON `null` (matching Jayway under
  /// `SUPPRESS_EXCEPTIONS`). Raises [InvalidJsonException] if the document is malformed inside its root value.
  /// The two short-lived arrays cost a few ns against a ~900ns parse, so the single-path case reuses the
  /// multi-path walk rather than duplicating it; split out a dedicated single-path walk only if a profile says so.
  @Nullable
  public static Object extract(String json, SimpleJsonPath path, boolean useBigDecimal, boolean earlyExit) {
    Object[] result = new Object[1];
    try (JsonParser parser = FACTORY.createParser(json)) {
      extract(parser, new SimpleJsonPath[]{path}, result, useBigDecimal, earlyExit);
    } catch (IOException e) {
      throw new InvalidJsonException(e);
    }
    return result[0];
  }

  /// UTF-8 overload of [#extract(String, SimpleJsonPath, boolean, boolean)] with the same return and exception
  /// contract.
  @Nullable
  public static Object extract(byte[] json, SimpleJsonPath path, boolean useBigDecimal, boolean earlyExit) {
    Object[] result = new Object[1];
    try (JsonParser parser = FACTORY.createParser(json)) {
      extract(parser, new SimpleJsonPath[]{path}, result, useBigDecimal, earlyExit);
    } catch (IOException e) {
      throw new InvalidJsonException(e);
    }
    return result[0];
  }

  /// Resolves every path in `paths` in one pass, writing path `i`'s value into `out[i]`. `out` must be at
  /// least as long as `paths` and is fully overwritten for the first `paths.length` entries.
  public static void extract(String json, SimpleJsonPath[] paths, Object[] out, boolean useBigDecimal,
      boolean earlyExit) {
    try (JsonParser parser = FACTORY.createParser(json)) {
      extract(parser, paths, out, useBigDecimal, earlyExit);
    } catch (IOException e) {
      throw new InvalidJsonException(e);
    }
  }

  /// UTF-8 overload of [#extract(String, SimpleJsonPath[], Object[], boolean, boolean)] with the same output and
  /// exception contract.
  public static void extract(byte[] json, SimpleJsonPath[] paths, Object[] out, boolean useBigDecimal,
      boolean earlyExit) {
    try (JsonParser parser = FACTORY.createParser(json)) {
      extract(parser, paths, out, useBigDecimal, earlyExit);
    } catch (IOException e) {
      throw new InvalidJsonException(e);
    }
  }

  private static void extract(JsonParser parser, SimpleJsonPath[] paths, Object[] out, boolean useBigDecimal,
      boolean earlyExit)
      throws IOException {
    int numPaths = paths.length;
    Preconditions.checkArgument(numPaths > 0 && numPaths <= MAX_PATHS, "Expected 1 to %s paths, got: %s", MAX_PATHS,
        numPaths);
    Preconditions.checkArgument(out.length >= numPaths, "Expected an output array of at least %s entries, got: %s",
        numPaths, out.length);
    for (int i = 0; i < numPaths; i++) {
      out[i] = null;
    }
    JsonToken token = parser.nextToken();
    if (token != JsonToken.START_OBJECT && token != JsonToken.START_ARRAY) {
      /// Guarded by canExtract(); a bare scalar addresses nothing under a non-empty linear path.
      return;
    }
    long live = numPaths == MAX_PATHS ? -1L : (1L << numPaths) - 1;
    walk(parser, paths, live, 0, out, useBigDecimal, earlyExit);
  }

  /// Walks the container the parser is currently positioned on, resolving the paths in `live` whose segment at
  /// `depth` matches. Consumes the container's closing token, unless it returns `true`, which means every path
  /// has resolved and the caller should stop reading immediately (early-exit mode only).
  private static boolean walk(JsonParser parser, SimpleJsonPath[] paths, long live, int depth, Object[] out,
      boolean useBigDecimal, boolean earlyExit)
      throws IOException {
    boolean isObject = parser.currentToken() == JsonToken.START_OBJECT;
    int index = 0;
    while (true) {
      JsonToken token = parser.nextToken();
      if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
        return false;
      }
      long leaves = 0;
      long children = 0;
      if (isObject) {
        String name = parser.currentName();
        parser.nextToken();
        for (long mask = live; mask != 0; mask &= mask - 1) {
          int i = Long.numberOfTrailingZeros(mask);
          SimpleJsonPath path = paths[i];
          if (!path.matchesKey(depth, name)) {
            continue;
          }
          /// Jackson keeps the last of a set of duplicate keys, so a later occurrence must discard whatever an
          /// earlier one resolved to - including values resolved deeper down inside it.
          out[i] = null;
          if (depth == path.length() - 1) {
            leaves |= 1L << i;
          } else {
            children |= 1L << i;
          }
        }
      } else {
        int elementIndex = index++;
        for (long mask = live; mask != 0; mask &= mask - 1) {
          int i = Long.numberOfTrailingZeros(mask);
          SimpleJsonPath path = paths[i];
          if (!path.matchesIndex(depth, elementIndex)) {
            continue;
          }
          if (depth == path.length() - 1) {
            leaves |= 1L << i;
          } else {
            children |= 1L << i;
          }
        }
      }

      JsonToken value = parser.currentToken();
      boolean container = value == JsonToken.START_OBJECT || value == JsonToken.START_ARRAY;
      if (leaves != 0) {
        /// At least one path ends here. Materialize the value once; any path that wants to descend further
        /// (e.g. both `$.a` and `$.a.b` were requested) navigates the materialized value instead of the stream.
        Object materialized = readValue(parser, value, useBigDecimal);
        for (long mask = leaves; mask != 0; mask &= mask - 1) {
          out[Long.numberOfTrailingZeros(mask)] = materialized;
        }
        for (long mask = children; mask != 0; mask &= mask - 1) {
          int i = Long.numberOfTrailingZeros(mask);
          out[i] = navigate(materialized, paths[i], depth + 1);
        }
        if (earlyExit && allResolved(out, paths.length)) {
          return true;
        }
      } else if (children != 0 && container) {
        if (walk(parser, paths, children, depth + 1, out, useBigDecimal, earlyExit)) {
          return true;
        }
      } else {
        /// Either nothing matches here, or a path wants to descend into a scalar - which resolves to null.
        parser.skipChildren();
      }
    }
  }

  /// Resolves the remaining segments of `path`, from `depth` onwards, against an already-materialized value.
  /// Mirrors Jayway's behavior on the same DOM: any type mismatch, missing key or out-of-range index is `null`.
  @Nullable
  private static Object navigate(@Nullable Object node, SimpleJsonPath path, int depth) {
    for (int d = depth, n = path.length(); d < n; d++) {
      String key = path.getKey(d);
      if (key != null) {
        if (!(node instanceof Map)) {
          return null;
        }
        node = ((Map<?, ?>) node).get(key);
      } else {
        if (!(node instanceof List)) {
          return null;
        }
        List<?> list = (List<?>) node;
        int index = path.getIndex(d);
        if (index >= list.size()) {
          return null;
        }
        node = list.get(index);
      }
    }
    return node;
  }

  /// Reads the value the parser is positioned on, leaving the parser on that value's last token. Containers are
  /// materialized through the same `ObjectMapper` configuration Jayway uses, so their contents match exactly.
  @Nullable
  private static Object readValue(JsonParser parser, JsonToken token, boolean useBigDecimal)
      throws IOException {
    switch (token) {
      case VALUE_STRING:
        return parser.getText();
      case VALUE_NUMBER_INT:
        return parser.getNumberValue();
      case VALUE_NUMBER_FLOAT:
        return useBigDecimal ? parser.getDecimalValue() : parser.getNumberValue();
      case VALUE_TRUE:
        return Boolean.TRUE;
      case VALUE_FALSE:
        return Boolean.FALSE;
      case VALUE_NULL:
        return null;
      case START_OBJECT:
      case START_ARRAY:
        return (useBigDecimal ? MAPPER_WITH_BIG_DECIMAL : MAPPER).readValue(parser, Object.class);
      default:
        throw new IllegalStateException("Unexpected JSON token: " + token);
    }
  }

  /// A path whose value is an explicit JSON `null` never counts as resolved, so early exit degrades to a full
  /// pass for those documents. That is the conservative direction: the extra scan can only add exceptions and
  /// later duplicates that a full pass would have found anyway.
  private static boolean allResolved(Object[] out, int numPaths) {
    for (int i = 0; i < numPaths; i++) {
      if (out[i] == null) {
        return false;
      }
    }
    return true;
  }
}
