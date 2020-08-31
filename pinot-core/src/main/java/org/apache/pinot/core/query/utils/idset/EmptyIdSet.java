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

/**
 * The {@code EmptyIdSet} represents an empty IdSet.
 */
public class EmptyIdSet implements IdSet {
  private EmptyIdSet() {
  }

  static final EmptyIdSet INSTANCE = new EmptyIdSet();

  @Override
  public Type getType() {
    return Type.EMPTY;
  }

  @Override
  public boolean contains(int id) {
    return false;
  }

  @Override
  public boolean contains(long id) {
    return false;
  }

  @Override
  public boolean contains(float id) {
    return false;
  }

  @Override
  public boolean contains(double id) {
    return false;
  }

  @Override
  public boolean contains(String id) {
    return false;
  }

  @Override
  public boolean contains(byte[] id) {
    return false;
  }

  @Override
  public int getSerializedSizeInBytes() {
    return 1;
  }

  @Override
  public byte[] toBytes() {
    // Single byte of IdSet.Type.EMPTY (0)
    return new byte[1];
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }
}
