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
package org.apache.pinot.segment.spi.index.creator;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


/**
 * Registration point to allow overriding of JsonIndexing
 */
public final class JsonIndexers {

  private JsonIndexers() {
  }

  private static final DefaultJsonIndexer DEFAULT_JSON_INDEXER = new DefaultJsonIndexer();
  private static final Function<JsonIndexer, JsonIndexer> DEFAULT = Function.identity();
  private static final AtomicReference<Function<JsonIndexer, JsonIndexer>> REGISTERED = new AtomicReference<>(DEFAULT);

  /**
   * Register a decorator which gets wraps the default JSON indexer so that it can intercept requests to
   * index JSON columns.
   * @param decorator wraps a JsonIndexer to create a new JsonIndexer with extra capabilities.
   * @return false if a decorator has already been registered.
   */
  public static boolean registerDecorator(Function<JsonIndexer, JsonIndexer> decorator) {
    return REGISTERED.compareAndSet(DEFAULT, decorator);
  }

  public static JsonIndexer newJsonIndexer() {
    return Holder.DECORATOR.apply(DEFAULT_JSON_INDEXER);
  }

  static final class Holder {
    public static final Function<JsonIndexer, JsonIndexer> DECORATOR = JsonIndexers.REGISTERED.get();
  }
}
