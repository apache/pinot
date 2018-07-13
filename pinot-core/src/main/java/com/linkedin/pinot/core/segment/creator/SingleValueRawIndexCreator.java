/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator;

/**
 * Interface for index creator for single-values with variable length (eg strings).
 * Implementations of this interface write raw data values (in potentially compressed formats),
 * as opposed to dictionary encoded index.
 */
public interface SingleValueRawIndexCreator extends ForwardIndexCreator {

  /**
   * This method creates an index for the given docId and int value.
   *
   * @param docId Document id
   * @param valueToIndex int value to index.
   */
  void index(int docId, int valueToIndex);

  /**
   * This method creates an index for the given docId and long value.
   *
   * @param docId Document id
   * @param valueToIndex long value to index.
   */
  void index(int docId, long valueToIndex);

  /**
   * This method creates an index for the given docId and float value.
   *
   * @param docId Document id
   * @param valueToIndex float value to index.
   */
  void index(int docId, float valueToIndex);

  /**
   * This method creates an index for the given docId and double value.
   *
   * @param docId Document id
   * @param valueToIndex double value to index.
   */
  void index(int docId, double valueToIndex);

  /**
   * This method creates an index for the given docId and String value.
   *
   * @param docId Document id
   * @param valueToIndex Variable length string value to index.
   */
  void index(int docId, String valueToIndex);

  /**
   * This method creates an index for the given docId and byte[] value.
   *
   * @param docId Document id
   * @param valueToIndex Variable length byte[] value to index.
   */
  void index(int docId, byte[] valueToIndex);

  /**
   * This method creates an index for the given docId and object value.
   * Object should be one of the primitive supported types.
   *
   * @param docId Document id
   * @param valueToIndex Object value to index.
   */
  void index(int docId, Object valueToIndex);
}
