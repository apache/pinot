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
package org.apache.pinot.core.io.reader;


public abstract class BaseMapSingleValueReader<T extends ReaderContext> implements
    MapSingleValueReader<T> {


  @Override
  public int getIntIntMap(int rowId, int[] keys, int[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getStringIntMap(int rowId, String[] keys, int[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getStringLongMap(int rowId, String[] keys, long[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getStringFloatMap(int rowId, String[] keys, float[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getStringDoubleMap(int rowId, String[] keys, double[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getStringStringMap(int rowId, String[] keys, String[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntKeySet(int rowId, int[] keys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getStringKeySet(int rowId, String[] keys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntValue(int row, int key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntValue(int row, int key, T context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public char getCharValue(int row, String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShortValue(int row, String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntValue(int row, String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntValue(int row, String key, T context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongValue(int row, String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongValue(int row, String key, T context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int row, String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int row, String key, T context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleValue(int row, String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleValue(int row, String key, T context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStringValue(int row, String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStringValue(int row, String key, T context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytesValue(int row, String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T createContext() {
    throw new UnsupportedOperationException();
  }


}
