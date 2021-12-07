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
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;


/**
 * Pluggable interface to mediate between different JSON storage structures
 * andJSON indexing.
 */
public interface JsonIndexer {
  /**
   * Abstracts the process of creating a JSON index from a forward index,
   * meaning that the forward index need not be literally JSON to be indexed
   * as such.
   * @param numDocs the number of docs to read from the forward index.
   * @param source the forward index
   * @param target the JSON index creator
   */
  <T extends ForwardIndexReaderContext> void index(int numDocs, ForwardIndexReader<T> source, JsonIndexCreator target)
      throws IOException;

  /**
   * Abstracts the process of creating a JSON index from a forward index,
   * meaning that the forward index need not be literally JSON to be indexed
   * as such.
   * @param numDocs the number of docs to read from the forward index.
   * @param source the forward index
   * @param target the JSON index creator
   */
  <T extends ForwardIndexReaderContext> void index(int numDocs, Dictionary dictionary, ForwardIndexReader<T> source,
      JsonIndexCreator target)
      throws IOException;
}
