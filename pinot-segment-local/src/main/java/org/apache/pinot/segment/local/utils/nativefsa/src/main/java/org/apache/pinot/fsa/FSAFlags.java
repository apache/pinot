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
package org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa;

import java.util.Set;

/**
 * FSA automaton flags. Where applicable, flags follow Daciuk's <code>fsa</code>
 * package.
 */
public enum FSAFlags {
  /** Daciuk: flexible FSA encoding. */
  FLEXIBLE(1 << 0),

  /** Daciuk: stop bit in use. */
  STOPBIT(1 << 1),

  /** Daciuk: next bit in use. */
  NEXTBIT(1 << 2),

  /** Daciuk: tails compression. */
  TAILS(1 << 3),

  /*
   * These flags are outside of byte range (never occur in Daciuk's FSA).
   */

  /**
   * The FSA contains right-language count numbers on states.
   * 
   * @see FSA#getRightLanguageCount(int)
   */
  NUMBERS(1 << 8),

  /**
   * The FSA supports legacy built-in separator and filler characters (Daciuk's
   * FSA package compatibility).
   */
  SEPARATORS(1 << 9);

  /**
   * Bit mask for the corresponding flag.
   */
  public final int bits;

  /** */
  private FSAFlags(int bits) {
    this.bits = bits;
  }

  /**
   * @param flags The bitset with flags. 
   * @return Returns <code>true</code> iff this flag is set in <code>flags</code>. 
   */
  public boolean isSet(int flags) {
    return (flags & bits) != 0;
  }

  /**
   * @param flags A set of flags to encode. 
   * @return Returns the set of flags encoded as packed <code>short</code>.
   */
  public static short asShort(Set<FSAFlags> flags) {
    short value = 0;
    for (FSAFlags f : flags) {
      value |= f.bits;
    }
    return value;
  }
}