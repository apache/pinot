/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.realtime.impl.dictionary;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public abstract class MutableDictionaryReader implements Dictionary {
  private BiMap<Integer, Object> dictionaryIdBiMap;
  protected FieldSpec spec;
  private final AtomicInteger dictionaryIdGenerator;
  private int dictionarySize = 0;

  public MutableDictionaryReader(FieldSpec spec) {
    this.spec = spec;
    this.dictionaryIdBiMap = HashBiMap.<Integer, Object> create();
    dictionaryIdGenerator = new AtomicInteger(0);
  }

  protected void addToDictionaryBiMap(Object val) {
    if (!dictionaryIdBiMap.inverse().containsKey(val)) {
      dictionaryIdBiMap.put(new Integer(dictionaryIdGenerator.incrementAndGet()), val);
      dictionarySize++;
      return;
    }
  }

  public int length() {
    return dictionarySize;
  }

  public boolean contains(Object o) {
    return dictionaryIdBiMap.containsKey(o);
  }

  protected Integer getIndexOfFromBiMap(Object val) {
    return dictionaryIdBiMap.inverse().get(val);
  }

  protected Object getRawValueFromBiMap(int dictionaryId) {
    return dictionaryIdBiMap.get(new Integer(dictionaryId));
  }

  @Override
  public int getInt(int dictionaryId) {
    return ((Integer) dictionaryIdBiMap.get(new Integer(dictionaryId))).intValue();
  }

  @Override
  public String getString(int dictionaryId) {
    return ((String) dictionaryIdBiMap.get(new Integer(dictionaryId)));
  }

  @Override
  public float getFloat(int dictionaryId) {
    return ((Float) dictionaryIdBiMap.get(new Integer(dictionaryId))).floatValue();
  }

  @Override
  public long getLong(int dictionaryId) {
    return ((Long) dictionaryIdBiMap.get(new Integer(dictionaryId))).longValue();
  }

  @Override
  public double getDouble(int dictionaryId) {
    return ((Double) dictionaryIdBiMap.get(new Integer(dictionaryId))).doubleValue();
  }

  public abstract void index(Object rawValue);

  public abstract int indexOf(Object rawValue);

  public abstract Object get(int dictionaryId);

  public abstract boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper);

  public abstract long getLongValue(int dictionaryId);

  public abstract double getDoubleValue(int dictionaryId);

  public abstract String toString(int dictionaryId);

  public void print() {
    System.out.println("************* printing dictionary for column : " + spec.getName() + " ***************");
    for (Integer key : dictionaryIdBiMap.keySet()) {
      System.out.println(key + "," + dictionaryIdBiMap.get(key));
    }
    System.out.println("************************************");
  }
}
