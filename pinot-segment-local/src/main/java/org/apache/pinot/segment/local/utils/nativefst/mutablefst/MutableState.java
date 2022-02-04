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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils.MutableFSTUtils;


/**
 * The fst's mutable state implementation.
 *
 * Holds its outgoing {@link MutableArc} objects in an ArrayList allowing additions/deletions
 */
public class MutableState {

  protected char _label;

  // Is terminal
  protected boolean _isTerminal;

  // Is first state
  protected boolean _isStartState;

  // Outgoing arcs
  private final ArrayList<MutableArc> _arcs;

  // Incoming arcs (at least states with arcs that are incoming to us)
  private final Set<MutableState> _incomingStates = Sets.newIdentityHashSet();

  /**
   * Default Constructor
   */
  public MutableState() {
    _arcs = Lists.newArrayList();
  }

  public MutableState(boolean isStartState) {
    _isStartState = isStartState;
    _arcs = Lists.newArrayList();
  }

  public boolean isTerminal() {
    return _isTerminal;
  }

  public boolean isStartState() {
    return _isStartState;
  }

  public char getLabel() {
    return _label;
  }

  public void setLabel(char label) {
    _label = label;
  }

  public void setIsTerminal(boolean isTerminal) {
    _isTerminal = isTerminal;
  }

  /**
   * Get the number of outgoing arcs
   */
  public int getArcCount() {
    return _arcs.size();
  }

  /**
   * Get an arc based on it's index the arcs ArrayList
   *
   * @param index the arc's index
   * @return the arc
   */
  public MutableArc getArc(int index) {
    return _arcs.get(index);
  }

  public List<MutableArc> getArcs() {
    return _arcs;
  }

  @Override
  public String toString() {
    return "(" + _label + ")";
  }

  // adds an arc but should only be used by MutableFst
  void addArc(MutableArc arc) {
      _arcs.add(arc);
    }

  void addIncomingState(MutableState inState) {
    if (inState == this) {
      return;
    }
    _incomingStates.add(inState);
  }

  void removeIncomingState(MutableState inState) {
    _incomingStates.remove(inState);
  }

  public Iterable<MutableState> getIncomingStates() {
    return _incomingStates;
  }

  @Override
  public boolean equals(Object o) {
    return MutableFSTUtils.stateEquals(this, o);
  }

  @Override
  public int hashCode() {
    int result = _label;
    long temp = 0;
    result = 31 * result * ((int) (temp ^ (temp >>> 32)));
    result = 31 * result + (_arcs != null ? _arcs.hashCode() : 0);
    return result;
  }
}
