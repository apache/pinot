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
package org.apache.pinot.segment.local.utils.nativefst;

/**
 * FST automaton flags. Where applicable, flags follow Daciuk's <code>FST</code>
 * package.
 */
public enum FSTFlags {
  /** Daciuk: flexible FST encoding. */
  FLEXIBLE(1 << 0),

  /** Daciuk: stop bit in use. */
  STOPBIT(1 << 1),

  /** Daciuk: next bit in use. */
  NEXTBIT(1 << 2),

  /*
   * These flags are outside of byte range (never occur in Daciuk's FST).
   */

  /**
   * The FST contains right-language count numbers on states.
   *
   * @see FST#getRightLanguageCount(int)
   */
  NUMBERS(1 << 8),

  /**
   * The FST supports legacy built-in separator and filler characters (Daciuk's
   * FST package compatibility).
   */
  SEPARATORS(1 << 9);

  /**
   * Bit mask for the corresponding flag.
   */
  public final int _bits;

  /** */
  FSTFlags(int bits) {
    this._bits = bits;
  }
}