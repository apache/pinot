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

/**
 * Pair of states.
 */
public class StatePair {
  State _parentState;
  State _firstState;
  State _secondState;

  StatePair(State parentState, State firstState, State s2) {
    this._parentState = parentState;
    this._firstState = firstState;
    this._secondState = s2;
  }

  /**
   * Constructs a new state pair.
   * @param firstState first state
   * @param s2 second state
   */
  public StatePair(State firstState, State s2) {
    this._firstState = firstState;
    this._secondState = s2;
  }

  /**
   * Checks for equality.
   * @param obj object to compare with
   * @return true if <tt>obj</tt> represents the same pair of states as this pair
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StatePair) {
      StatePair p = (StatePair) obj;
      return p._firstState == _firstState && p._secondState == _secondState;
    } else {
      return false;
    }
  }

  /**
   * Returns hash code.
   * @return hash code
   */
  @Override
  public int hashCode() {
    return _firstState.hashCode() + _secondState.hashCode();
  }
}
