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
package org.apache.pinot.core.io.readerwriter;

import org.apache.pinot.core.io.reader.ForwardIndexReader;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.core.io.writer.ForwardIndexWriter;


/**
 * Interface for forward index reader-writer.
 */
public interface ForwardIndexReaderWriter extends ForwardIndexReader<ReaderContext>, ForwardIndexWriter {

  /**
   * Returns the length (size in bytes) of the shortest elements inside the forward index.
   *
   * @return The length (size in bytes) of the shortest elements inside the forward index.
   */
  int getLengthOfShortestElement();

  /**
   * Returns the length (size in bytes) of the longest elements inside the forward index.
   *
   * @return The length (size in bytes) of the longest elements inside the forward index.
   */
  int getLengthOfLongestElement();
}
