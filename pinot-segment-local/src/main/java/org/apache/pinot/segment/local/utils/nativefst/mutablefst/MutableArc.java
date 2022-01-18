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

package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils.MutableFSTUtils;


/**
 * A mutable FST's arc
 */
public class MutableArc {

  private int _outputSymbol;
  private MutableState _nextState;

  /**
   * Arc Constructor
   *
   * @param nextState the arc's next state
   */
  public MutableArc(int outputSymbol, MutableState nextState) {
    _outputSymbol = outputSymbol;
    _nextState = nextState;
  }

  public int getOutputSymbol() {
    return _outputSymbol;
  }

  /**
   * Get the next state
   */
  public MutableState getNextState() {
    return _nextState;
  }

  @Override
  public boolean equals(Object obj) {
    return MutableFSTUtils.arcEquals(this, obj);
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + (_nextState != null ? _nextState.getLabel() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "(" + _nextState.toString() + ")";
  }
}
