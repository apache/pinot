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
package org.apache.pinot.query.planner.partitioning;

import javax.annotation.Nullable;


public class EmptyKeySelector implements KeySelector<Integer> {
  private final String _hashFunction;

  private EmptyKeySelector() {
    this(KeySelector.DEFAULT_HASH_ALGORITHM);
  }

  private EmptyKeySelector(String hashFunction) {
    _hashFunction = hashFunction;
  }

  public static final EmptyKeySelector INSTANCE = new EmptyKeySelector();

  public static EmptyKeySelector getInstance(String hashFunction) {
    if (KeySelector.DEFAULT_HASH_ALGORITHM.equals(hashFunction)) {
      return INSTANCE;
    }
    return new EmptyKeySelector(hashFunction);
  }

  @Nullable
  @Override
  public Integer getKey(Object[] row) {
    return null;
  }

  @Override
  public int computeHash(Object[] input) {
    return 0;
  }

  @Override
  public String hashAlgorithm() {
    return _hashFunction;
  }
}
