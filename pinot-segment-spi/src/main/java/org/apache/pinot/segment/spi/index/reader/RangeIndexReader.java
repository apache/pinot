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
package org.apache.pinot.segment.spi.index.reader;

import java.io.Closeable;

/**
 * Interface for indexed range queries
 * @param <T>
 */
public interface RangeIndexReader<T> extends Closeable {
  /**
   * Returns doc ids with a value between min and max, both inclusive.
   * @param min the inclusive lower bound.
   * @param max the inclusive upper bound.
   * @return the matching doc ids.
   */
  T getMatchingDocIds(long min, long max);

  /**
   * Returns doc ids with a value between min and max, both inclusive.
   * @param min the inclusive lower bound.
   * @param max the inclusive upper bound.
   * @return the matching doc ids.
   */
  T getMatchingDocIds(int min, int max);

  /**
   * Returns doc ids with a value between min and max, both inclusive.
   * @param min the inclusive lower bound.
   * @param max the inclusive upper bound.
   * @return the matching doc ids.
   */
  T getMatchingDocIds(double min, double max);

  /**
   * Returns doc ids with a value between min and max, both inclusive.
   * @param min the inclusive lower bound.
   * @param max the inclusive upper bound.
   * @return the matching doc ids.
   */
  T getMatchingDocIds(float min, float max);

  /**
   * Returns doc ids with a value between min and max, both inclusive.
   * @param min the inclusive lower bound.
   * @param max the inclusive upper bound.
   * @return the matching doc ids.
   */
  T getPartiallyMatchingDocIds(long min, long max);

  /**
   * Returns doc ids with a value between min and max, both inclusive.
   * @param min the inclusive lower bound.
   * @param max the inclusive upper bound.
   * @return the matching doc ids.
   */
  T getPartiallyMatchingDocIds(int min, int max);

  /**
   * Returns doc ids with a value between min and max, both inclusive.
   * @param min the inclusive lower bound.
   * @param max the inclusive upper bound.
   * @return the matching doc ids.
   */
  T getPartiallyMatchingDocIds(double min, double max);

  /**
   * Returns doc ids with a value between min and max, both inclusive.
   * @param min the inclusive lower bound.
   * @param max the inclusive upper bound.
   * @return the matching doc ids.
   */
  T getPartiallyMatchingDocIds(float min, float max);
}
