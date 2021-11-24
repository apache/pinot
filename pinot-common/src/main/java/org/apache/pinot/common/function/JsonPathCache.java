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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.spi.cache.Cache;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;


public class JsonPathCache implements Cache {

  public static final JsonPathCache INSTANCE = new JsonPathCache();

  private static final long DEFAULT_CACHE_MAXIMUM_SIZE = 10000;
  private static final Predicate[] NO_PREDICATES = new Predicate[0];

  private final LoadingCache<String, JsonPath> _jsonPathCache =
      CacheBuilder.newBuilder().maximumSize(DEFAULT_CACHE_MAXIMUM_SIZE)
          .build(new CacheLoader<String, JsonPath>() {
            @Override
            public JsonPath load(@Nonnull String jsonPath) {
              return JsonPath.compile(jsonPath, NO_PREDICATES);
            }
          });

  @Override
  public JsonPath get(String key) {
    return _jsonPathCache.getIfPresent(key);
  }

  public JsonPath getOrCompute(String key) {
    try {
      return _jsonPathCache.get(key);
    } catch (ExecutionException e) {
      // the cast is safe because JsonPath.compile only throws RuntimeExceptions
      throw (RuntimeException) e.getCause();
    }
  }

  @Override
  public void put(String key, JsonPath value) {
    _jsonPathCache.put(key, value);
  }

  @VisibleForTesting
  public long size() {
    return _jsonPathCache.size();
  }
}
