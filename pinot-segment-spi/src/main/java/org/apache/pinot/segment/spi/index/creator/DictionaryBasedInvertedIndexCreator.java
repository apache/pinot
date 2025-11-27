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

/**
 * Support for RoaringBitmap inverted index:
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
 *
 * <p>To create an inverted index:
 * <ul>
 *   <li>R
 *     Construct an instance of <code>InvertedIndexCreator</code>
 *   </li>
 *   <li>
 *     Call add() for each docId in sequence starting with 0 to add dictId (dictIds for multi-valued column) into the
 *     creator
 *   </li>
 *   <li>
 *     Call seal() after all dictIds have been added
 *   </li>
 * </ul>
 *
 * Support for Lucene based inverted index for text
 */
public interface DictionaryBasedInvertedIndexCreator extends InvertedIndexCreator {

  @Override
  default void add(Object value, int dictId) {
    assert dictId >= 0;
    add(dictId);
  }

  @Override
  default void add(Object[] values, int[] dictIds) {
    assert dictIds != null;
    add(dictIds, dictIds.length);
  }

  /**
   * Primitive type additions for columnar processing optimization.
   * These methods avoid boxing overhead when iterating over columnar data.
   */

  @Override
  default void addInt(int value, int dictId) {
    add(dictId);
  }

  @Override
  default void addLong(long value, int dictId) {
    add(dictId);
  }

  @Override
  default void addFloat(float value, int dictId) {
    add(dictId);
  }

  @Override
  default void addDouble(double value, int dictId) {
    add(dictId);
  }

  @Override
  default void addString(String value, int dictId) {
    add(dictId);
  }

  @Override
  default void addBytes(byte[] value, int dictId) {
    add(dictId);
  }

  @Override
  default void addIntMV(int[] values, int[] dictIds) {
    add(dictIds, dictIds.length);
  }

  @Override
  default void addLongMV(long[] values, int[] dictIds) {
    add(dictIds, dictIds.length);
  }

  @Override
  default void addFloatMV(float[] values, int[] dictIds) {
    add(dictIds, dictIds.length);
  }

  @Override
  default void addDoubleMV(double[] values, int[] dictIds) {
    add(dictIds, dictIds.length);
  }

  @Override
  default void addStringMV(String[] values, int[] dictIds) {
    add(dictIds, dictIds.length);
  }

  @Override
  default void addBytesMV(byte[][] values, int[] dictIds) {
    add(dictIds, dictIds.length);
  }

  /**
   * For single-value column, adds the dictionary id for the next document.
   */
  void add(int dictId);

  /**
   * For multi-value column, adds the dictionary ids for the next document.
   */
  void add(int[] dictIds, int length);
}
