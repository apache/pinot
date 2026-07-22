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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;


/// A JsonPath expression restricted to a *simple linear chain*: a `$` root followed only by object-key and
/// array-index steps. Such a path addresses at most one value and can be resolved by a single forward pass
/// over the JSON document, which is what [FastJsonPathExtractor] does.
///
/// Accepted syntax:
/// - `.name` where `name` is made of letters, digits, `_` or `-`
/// - `['name']` or `["name"]` — the quoted key is taken literally (a `.` inside it does **not** nest)
/// - `[123]` — a non-negative array index
///
/// Everything else compiles to `null` and must fall back to Jayway: wildcards (`*`), deep scan (`..`),
/// filters (`[?(...)]`), unions (`['a','b']`), slices (`[1:2]`), functions, negative indices (`[-1]`, which
/// Jayway resolves from the end and therefore needs the array length), bracket keys containing a backslash
/// (Jayway's bracket-escape handling is not reproducible here), a bare `$`, and any path not starting
/// with `$`.
///
/// Compilation results are cached per literal path string; in practice a path is a query or transform-config
/// literal, so the cache is warm after the first row.
///
/// The cache is a plain [ConcurrentHashMap] rather than a Guava `CacheBuilder.maximumSize` cache on purpose:
/// this is read on the per-row path, and Guava's size-bounded cache takes a segment lock on every *read* to
/// maintain its LRU recency queue, which measurably contends across query threads. Growth is bounded instead
/// by simply not admitting new entries past [#CACHE_MAXIMUM_SIZE] - a path that misses a full cache is
/// re-parsed, which is a few dozen character comparisons, so the fallback is cheap and there is no eviction
/// policy to maintain.
///
/// Immutable and thread-safe.
public final class SimpleJsonPath {
  private static final int CACHE_MAXIMUM_SIZE = 10_000;

  /// Sentinel for "not a simple linear path". Distinguishable from any real instance because a real one always
  /// has at least one segment (a bare `$` is deliberately rejected).
  private static final SimpleJsonPath NOT_SIMPLE = new SimpleJsonPath(new String[0], new int[0]);

  private static final ConcurrentHashMap<String, SimpleJsonPath> CACHE = new ConcurrentHashMap<>();

  /// Segment `i` is an object key when `_keys[i] != null`, and an array index (`_indices[i]`) otherwise.
  private final String[] _keys;
  private final int[] _indices;

  private SimpleJsonPath(String[] keys, int[] indices) {
    _keys = keys;
    _indices = indices;
  }

  /// Returns the compiled path, or `null` when `jsonPath` is `null` or is not a simple linear chain, in which
  /// case the caller must fall back to Jayway. A `null` path is deliberately reported the same way rather than
  /// raising, so that the fast path never changes which exception a caller sees - Jayway raises its own.
  @Nullable
  public static SimpleJsonPath compile(@Nullable String jsonPath) {
    if (jsonPath == null) {
      return null;
    }
    SimpleJsonPath compiled = CACHE.get(jsonPath);
    if (compiled == null) {
      compiled = parse(jsonPath);
      /// Bound the cache by refusing new entries when full: query paths are attacker-controllable, and re-parsing
      /// is cheap enough that an eviction policy would cost more than the miss it saves.
      if (CACHE.size() < CACHE_MAXIMUM_SIZE) {
        CACHE.putIfAbsent(jsonPath, compiled);
      }
    }
    return compiled == NOT_SIMPLE ? null : compiled;
  }

  int length() {
    return _keys.length;
  }

  /// Returns the object key at `depth`, or `null` when that segment is an array index.
  @Nullable
  String getKey(int depth) {
    return _keys[depth];
  }

  /// Returns the array index at `depth`. Only meaningful when [#getKey] returns `null` for the same depth.
  int getIndex(int depth) {
    return _indices[depth];
  }

  boolean matchesKey(int depth, String key) {
    return key.equals(_keys[depth]);
  }

  boolean matchesIndex(int depth, int index) {
    return _keys[depth] == null && _indices[depth] == index;
  }

  @VisibleForTesting
  static SimpleJsonPath parse(String jsonPath) {
    int length = jsonPath.length();
    if (length < 2 || jsonPath.charAt(0) != '$') {
      return NOT_SIMPLE;
    }
    List<String> keys = new ArrayList<>(4);
    List<Integer> indices = new ArrayList<>(4);
    int pos = 1;
    while (pos < length) {
      char c = jsonPath.charAt(pos);
      if (c == '.') {
        int start = ++pos;
        while (pos < length && isNameChar(jsonPath.charAt(pos))) {
          pos++;
        }
        if (pos == start) {
          /// "..", ".*", a trailing "." or a name starting with an unsupported character.
          return NOT_SIMPLE;
        }
        keys.add(jsonPath.substring(start, pos));
        indices.add(-1);
      } else if (c == '[') {
        pos++;
        if (pos == length) {
          return NOT_SIMPLE;
        }
        char quote = jsonPath.charAt(pos);
        if (quote == '\'' || quote == '"') {
          int start = ++pos;
          while (pos < length && jsonPath.charAt(pos) != quote) {
            if (jsonPath.charAt(pos) == '\\') {
              return NOT_SIMPLE;
            }
            pos++;
          }
          if (pos == length) {
            return NOT_SIMPLE;
          }
          String key = jsonPath.substring(start, pos);
          /// Skip the closing quote; anything other than ']' next means a union such as ['a','b'].
          if (++pos == length || jsonPath.charAt(pos++) != ']') {
            return NOT_SIMPLE;
          }
          keys.add(key);
          indices.add(-1);
        } else {
          int start = pos;
          while (pos < length && isDigit(jsonPath.charAt(pos))) {
            pos++;
          }
          /// Rejects [*], [?(...)], [-1], [1,2] and [1:2] because none of them close right after the digits.
          if (pos == start || pos == length || jsonPath.charAt(pos++) != ']') {
            return NOT_SIMPLE;
          }
          long index;
          try {
            index = Long.parseLong(jsonPath, start, pos - 1, 10);
          } catch (NumberFormatException e) {
            return NOT_SIMPLE;
          }
          if (index > Integer.MAX_VALUE) {
            return NOT_SIMPLE;
          }
          keys.add(null);
          indices.add((int) index);
        }
      } else {
        return NOT_SIMPLE;
      }
    }
    if (keys.isEmpty()) {
      return NOT_SIMPLE;
    }
    int size = keys.size();
    int[] indexArray = new int[size];
    for (int i = 0; i < size; i++) {
      indexArray[i] = indices.get(i);
    }
    return new SimpleJsonPath(keys.toArray(new String[0]), indexArray);
  }

  private static boolean isNameChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '-';
  }

  private static boolean isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("$");
    for (int i = 0; i < _keys.length; i++) {
      if (_keys[i] != null) {
        sb.append("['").append(_keys[i]).append("']");
      } else {
        sb.append('[').append(_indices[i]).append(']');
      }
    }
    return sb.toString();
  }
}
