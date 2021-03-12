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

package org.apache.pinot.controller.recommender.data.generator;

import org.apache.commons.codec.binary.Hex;


/**
 * A class to generating data for a column with type BYTES
 */
public class BytesGenerator implements Generator {
  private static final double DEFAULT_NUMBER_OF_VALUES_PER_ENTRY = 1;

  private final StringGenerator _stringGenerator;

  public BytesGenerator(Integer cardinality, Integer entryLength) {
    _stringGenerator = new StringGenerator(cardinality, DEFAULT_NUMBER_OF_VALUES_PER_ENTRY, entryLength);
  }

  @Override
  public void init() {
    _stringGenerator.init();
  }

  @Override
  public Object next() {
    String next = (String) _stringGenerator.next();
    return Hex.encodeHexString(next.getBytes());
  }
}
