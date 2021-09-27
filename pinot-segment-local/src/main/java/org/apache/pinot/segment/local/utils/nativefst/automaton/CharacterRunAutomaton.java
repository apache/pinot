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

package org.apache.pinot.segment.local.utils.nativefst.automaton;

/** Automaton representation for matching char[]. */
public class CharacterRunAutomaton extends RunAutomaton {

  /**
   * Constructor specifying determinizeWorkLimit.
   *
   * @param a Automaton to match
   */
  public CharacterRunAutomaton(Automaton a) {
    super(a, true);
  }

  public boolean run(String s) {
    int p = 0;
    int l = s.length();
    int i = 0;

    int cp;
    for (; i < l; i += Character.charCount(cp)) {
      cp = s.codePointAt(i);
      p = step(p, cp);
      if (p == -1) {
        return false;
      }
    }

    return _accept[p];
  }

  public boolean run(char[] s, int offset, int length) {
    int p = 0;
    int l = offset + length;
    int i = offset;

    int cp;
    for (; i < l; i += Character.charCount(cp)) {
      cp = Character.codePointAt(s, i, l);
      p = step(p, cp);
      if (p == -1) {
        return false;
      }
    }

    return _accept[p];
  }
}
