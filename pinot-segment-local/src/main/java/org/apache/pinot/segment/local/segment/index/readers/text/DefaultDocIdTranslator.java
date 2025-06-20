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
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.IOException;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Lucene docIDs are not same as pinot docIDs. The internal implementation
 * of Lucene can change the docIds and they are not guaranteed to be the
 * same as how we expect -- strictly increasing docIDs as the documents
 * are ingested during segment/index creation.
 * This class is used to map the luceneDocId (returned by the search query
 * to the collector) to corresponding pinotDocId.
 */
class DefaultDocIdTranslator implements DocIdTranslator {
  final PinotDataBuffer _buffer;

  DefaultDocIdTranslator(PinotDataBuffer buffer) {
    _buffer = buffer;
  }

  public int getPinotDocId(int luceneDocId) {
    return _buffer.getInt(luceneDocId * Integer.BYTES);
  }

  @Override
  public void close()
      throws IOException {
    _buffer.close();
  }
}
