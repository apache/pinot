/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.Closeable;
import java.io.IOException;


/**
 * Currently only support RoaringBitmap inverted index.
 * <pre>
 * Layout for RoaringBitmap inverted index:
 * |-------------------------------------------------------------------------|
 * |                    Start offset of 1st bitmap                           |
 * |    End offset of 1st bitmap (exclusive) / Start offset of 2nd bitmap    |
 * |                                   ...                                   |
 * | End offset of 2nd last bitmap (exclusive) / Start offset of last bitmap |
 * |                  End offset of last bitmap (exclusive)                  |
 * |-------------------------------------------------------------------------|
 * |                           Data for 1st bitmap                           |
 * |                           Data for 2nd bitmap                           |
 * |                                   ...                                   |
 * |                           Data for last bitmap                          |
 * |-------------------------------------------------------------------------|
 * </pre>
 */
public interface InvertedIndexCreator extends Closeable {

  /**
   * Add an entry for single-value column.
   *
   * @param docId Document id
   * @param dictId Dictionary id
   */
  void addSV(int docId, int dictId);

  /**
   * Add an entry for multi-value column.
   *
   * @param docId Document id
   * @param dictIds Array of dictionary ids
   */
  void addMV(int docId, int[] dictIds);

  /**
   * Add an entry for multi-value column.
   *
   * @param docId Document id
   * @param dictIds Array of dictionary ids
   * @param numDictIds Number of dictionary ids
   */
  void addMV(int docId, int[] dictIds, int numDictIds);

  /**
   * Seal the results into the file.
   *
   * @throws IOException
   */
  void seal() throws IOException;
}
