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
package org.apache.pinot.core.bloom;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * Interface for bloom filter
 */
public interface BloomFilter {

  /**
   * Get the version of bloom filter implementation
   * @return a version
   */
  int getVersion();

  /**
   * Get the type of the bloom filter
   * @return a bloom filter type
   */
  BloomFilterType getBloomFilterType();

  /**
   * Add element to bloom filter
   *
   * @param input input object
   */
  void add(Object input);

  /**
   * Check if the input element may exist or not
   *
   * @param input input object for testing
   * @return true if the input may exist, false if it does not exist
   */
  boolean mightContain(Object input);

  /**
   * Serialize bloom filter to output stream.
   *
   * @param out output stream
   * @throws IOException
   */
  void writeTo(OutputStream out)
      throws IOException;

  /**
   * Deserialize the bloom filter from input stream.
   * @param in input stream
   * @throws IOException
   */
  void readFrom(InputStream in)
      throws IOException;
}
