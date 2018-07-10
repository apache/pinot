/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.groupby.utils;

/**
 * Abstract base class for {@link ValueToIdMap} interface.
 */
public abstract class BaseValueToIdMap implements ValueToIdMap {
  @Override
  public int put(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int put(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int put(float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int put(double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int put(String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int id) {
    throw new UnsupportedOperationException();
  }
}
