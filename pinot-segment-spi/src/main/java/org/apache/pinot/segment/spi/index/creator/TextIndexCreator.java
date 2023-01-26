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
import org.apache.pinot.segment.spi.index.IndexCreator;


/**
 * Index creator for both text and FST indexes.
 *
 * This abstraction is not great. Text and FST indexes are quite different and in fact they way they are created is not
 * compatible. These incompatibilities between indexes affect this shared creator itself, breaking SOLID in at least the
 * Liskov substitution principle and Dependency inversion principle.
 * For example, while Text indexes are created as normal indexes adding new entries for each row, FST indexes are built
 * using the sorted set of unique values that is precalculated. Therefore there is nothing in common between these
 * indexes (apart from dealing with Strings) and they ideally should not share a common interface for the same reason
 * BloomFilterCreator does not implement this interface.
 */
public interface TextIndexCreator extends IndexCreator {

  /**
   * Adds the next document.
   */
  void add(String document);

  /**
   * Adds a set of documents to the index
   */
  void add(String[] document, int length);

  /**
   * Seals the index and flushes it to disk.
   */
  void seal()
      throws IOException;
}
