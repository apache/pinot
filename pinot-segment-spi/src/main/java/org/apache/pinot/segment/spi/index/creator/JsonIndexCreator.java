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

import java.io.Closeable;
import java.io.IOException;


/**
 * Index creator for json index.
 */
public interface JsonIndexCreator extends Closeable {
  char KEY_VALUE_SEPARATOR = '\0';

  /**
   * To be invoked once before each physical JSON document when streaming JSON into the index
   * @param numFlattenedDocs the number of flattened documents to be streamed into the index
   */
  default void startDocument(int numFlattenedDocs) {
  }

  /**
   * To be invoked after each JSON document when streaming JSON into the index.
   */
  default void endDocument() {
  }

  /**
   * To be invoked once before each flattened document when streaming JSON into the index
   */
  default void startFlattenedDocument() {
  }

  /**
   * To be invoked once after each flattened JSON document when streaming JSON into the index
   */
  default void endFlattenedDocument() {
  }

  /**
   * Stream JSON fragments into the index.
   * @param path the full path
   * @param value the value
   */
  void add(String path, String value);

  /**
   * Adds a json value
   */
  void add(String jsonString)
      throws IOException;

  /**
   * Seals the index and flushes it to disk.
   */
  void seal()
      throws IOException;
}
