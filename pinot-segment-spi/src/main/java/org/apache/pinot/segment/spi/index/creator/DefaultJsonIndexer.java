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
 * A JSON indexer which just assumes the forward index contains JSON.
 * {@see JsonIndexer}
 */
public final class DefaultJsonIndexer implements JsonIndexer {

  @Override
  public <T extends ForwardIndexReaderContext> void index(int numDocs, ForwardIndexReader<T> source,
      JsonIndexCreator target)
      throws IOException {
    try (T context = source.createContext()) {
      for (int docId = 0; docId < numDocs; docId++) {
        target.add(source.getString(docId, context));
      }
    }
    target.seal();
  }

  @Override
  public <T extends ForwardIndexReaderContext> void index(int numDocs, Dictionary dictionary,
      ForwardIndexReader<T> source, JsonIndexCreator target)
      throws IOException {
    try (T context = source.createContext()) {
      for (int docId = 0; docId < numDocs; docId++) {
        target.add(dictionary.getStringValue(source.getDictId(docId, context)));
      }
    }
    target.seal();
  }
}
