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

import com.google.common.hash.Funnels;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;


/**
 * Bloom filter implementation with guava library
 */
public class GuavaOnHeapBloomFilter implements BloomFilter {
  // Increment the version when the bloom filter implementation becomes backward incompatible
  private static final int VERSION = 1;

  private com.google.common.hash.BloomFilter _bloomFilter;

  public GuavaOnHeapBloomFilter() {
  }

  public GuavaOnHeapBloomFilter(int cardinality, double maxFalsePosProbability) {
    _bloomFilter = com.google.common.hash.BloomFilter
        .create(Funnels.stringFunnel(Charset.forName("UTF-8")), cardinality, maxFalsePosProbability);
  }

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public BloomFilterType getBloomFilterType() {
    return BloomFilterType.GUAVA_ON_HEAP;
  }

  @Override
  public void add(Object input) {
    _bloomFilter.put(input.toString());
  }

  @Override
  public boolean mightContain(Object input) {
    return _bloomFilter.mightContain(input.toString());
  }

  @Override
  public void writeTo(OutputStream out)
      throws IOException {
    _bloomFilter.writeTo(out);
  }

  @Override
  public void readFrom(InputStream in)
      throws IOException {
    _bloomFilter = com.google.common.hash.BloomFilter.readFrom(in, Funnels.stringFunnel(Charset.forName("UTF-8")));
  }
}
