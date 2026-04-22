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

import java.io.IOException;
import java.util.Map;
import org.apache.pinot.segment.spi.index.IndexCreator;


/**
 * Creator for the ColumnarMap index. Accepts a Map per document during segment creation and
 * decomposes it into per-key columnar storage (presence bitmaps, typed forward indexes,
 * optional inverted indexes) on seal().
 */
public interface ColumnarMapIndexCreator extends IndexCreator {

  /**
   * Adds a document's sparse map data. The map may be null or empty if the document has no keys.
   * Keys in the map that match declared key types are stored with typed per-key forward indexes.
   * Undeclared keys are stored using the default value type (STRING if not configured).
   */
  void add(Map<String, Object> columnarMap)
      throws IOException;

  /**
   * Finalizes the index, writing per-key presence bitmaps, typed forward indexes, and optional
   * inverted indexes to the index file.
   */
  @Override
  void seal()
      throws IOException;
}
