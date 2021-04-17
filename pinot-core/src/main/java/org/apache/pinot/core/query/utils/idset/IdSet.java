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
package org.apache.pinot.core.query.utils.idset;

import java.io.IOException;
import java.util.Base64;


/**
 * The {@code IdSet} represents a collection of ids. It can be used to optimize the query with huge IN clause.
 */
public interface IdSet {

  enum Type {
    // DO NOT change the ids as the ser/de relies on them
    EMPTY((byte) 0),
    ROARING_BITMAP((byte) 1),
    ROARING_64_NAVIGABLE_MAP((byte) 2),
    BLOOM_FILTER((byte) 3);

    private final byte _id;

    Type(byte id) {
      _id = id;
    }

    public byte getId() {
      return _id;
    }
  }

  /**
   * Returns the type of the IdSet.
   */
  Type getType();

  /**
   * Adds an INT id into the IdSet.
   */
  default void add(int id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Adds a LONG id into the IdSet.
   */
  default void add(long id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Adds a FLOAT id into the IdSet.
   */
  default void add(float id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Adds a DOUBLE id into the IdSet.
   */
  default void add(double id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Adds a STRING id into the IdSet.
   */
  default void add(String id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Adds a BYTES id into the IdSet.
   */
  default void add(byte[] id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns {@code true} if the IdSet contains the given INT id, {@code false} otherwise.
   */
  default boolean contains(int id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns {@code true} if the IdSet contains the given LONG id, {@code false} otherwise.
   */
  default boolean contains(long id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns {@code true} if the IdSet contains the given FLOAT id, {@code false} otherwise.
   */
  default boolean contains(float id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns {@code true} if the IdSet contains the given DOUBLE id, {@code false} otherwise.
   */
  default boolean contains(double id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns {@code true} if the IdSet contains the given STRING id, {@code false} otherwise.
   */
  default boolean contains(String id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns {@code true} if the IdSet contains the given BYTES id, {@code false} otherwise.
   */
  default boolean contains(byte[] id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the number of bytes required to serialize the IdSet.
   */
  int getSerializedSizeInBytes();

  /**
   * Serializes the IdSet into a byte array.
   */
  byte[] toBytes() throws IOException;

  /**
   * Serializes the IdSet into a Base64 string.
   * <p>Use Base64 instead of Hex encoding for better compression.
   */
  default String toBase64String() throws IOException {
    return Base64.getEncoder().encodeToString(toBytes());
  }
}
