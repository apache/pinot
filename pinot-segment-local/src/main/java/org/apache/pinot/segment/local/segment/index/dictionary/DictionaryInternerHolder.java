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
package org.apache.pinot.segment.local.segment.index.dictionary;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.utils.FALFInterner;


/**
 * This class holds the dictionary interners. It is currently used only for OnHeapStringDictionary.
 */
public class DictionaryInternerHolder {
  private static final DictionaryInternerHolder INSTANCE = new DictionaryInternerHolder();

  // Map containing tableName + columnName as key and the interner as value. The interner is common across all the
  // segments for a given table.
  private Map<String, FALFInterner<String>> _strInternerInfoMap;
  private Map<String, FALFInterner<byte[]>> _byteInternerInfoMap;

  private DictionaryInternerHolder() {
    _strInternerInfoMap = new ConcurrentHashMap<>();
    _byteInternerInfoMap = new ConcurrentHashMap<>();
  }

  public static DictionaryInternerHolder getInstance() {
    return INSTANCE;
  }

  public FALFInterner<String> getStrInterner(String columnIdentifier, int capacity) {
    return _strInternerInfoMap.computeIfAbsent(columnIdentifier, k -> new FALFInterner<>(capacity));
  }

  public FALFInterner<byte[]> getByteInterner(String columnIdentifier, int capacity) {
    return _byteInternerInfoMap.computeIfAbsent(columnIdentifier, k -> new FALFInterner<>(capacity, Arrays::hashCode));
  }

  public String createIdentifier(String tableName, String colName) {
    return tableName + ":" + colName;
  }
}
