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
package org.apache.pinot.core.segment.processing.collector;

import java.util.Iterator;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Collects and stores GenericRows
 */
public interface Collector {

  /**
   * Collects the given GenericRow and stores it
   * @param genericRow the generic row to add to the collection
   */
  void collect(GenericRow genericRow);

  /**
   * Provides an iterator for the GenericRows in the collection
   */
  Iterator<GenericRow> iterator();

  /**
   * The size of the collection
   */
  int size();

  /**
   * Resets the collection of this collector by deleting all existing GenericRows
   */
  void reset();
}
