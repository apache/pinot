/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils.MutableFSTUtils;

import java.util.ArrayList;

import java.util.List;
import java.util.Set;


/**
 * The fst's mutable state implementation.
 *
 * Holds its outgoing {@link MutableArc} objects in an ArrayList allowing additions/deletions
 */
public class MutableState {

  protected char label;

  // Is terminal
  protected boolean isTerminal;

  // Is first state
  protected boolean isStartState;

  // Outgoing arcs
  private final ArrayList<MutableArc> arcs;

  // Incoming arcs (at least states with arcs that are incoming to us)
  private final Set<MutableState> incomingStates = Sets.newIdentityHashSet();

  /**
   * Default Constructor
   */
  public MutableState() {
    arcs = Lists.newArrayList();
  }

  public MutableState(boolean isStartState) {
    this.isStartState = isStartState;
    arcs = Lists.newArrayList();
  }

  public boolean isTerminal() {
    return isTerminal;
  }

  public boolean isStartState() {
    return isStartState;
  }

  public char getLabel() {
    return label;
  }

  public void setLabel(char label) {
    this.label = label;
  }

  public void setIsTerminal(boolean isTerminal) {
    this.isTerminal = isTerminal;
  }

  /**
   * Get the number of outgoing arcs
   */
  public int getArcCount() {
    return this.arcs.size();
  }

  /**
   * Get an arc based on it's index the arcs ArrayList
   *
   * @param index the arc's index
   * @return the arc
   */
  public MutableArc getArc(int index) {
    return this.arcs.get(index);
  }


  public List<MutableArc> getArcs() {
    return this.arcs;
  }

  @Override
  public String toString() {
    return "(" + label +  ")";
  }

  /* friend methods to let the fst maintain state's state */

  // adds an arc but should only be used by MutableFst
  void addArc(MutableArc arc) {
      this.arcs.add(arc);
    }

  void addIncomingState(MutableState inState) {
    if (inState == this) return;
    this.incomingStates.add(inState);
  }

  void removeIncomingState(MutableState inState) {
    this.incomingStates.remove(inState);
  }

  public Iterable<MutableState> getIncomingStates() {
    return this.incomingStates;
  }

  @Override
  public boolean equals(Object o) {
    return MutableFSTUtils.stateEquals(this, o);
  }

  @Override
  public int hashCode() {
    int result = label;
    long temp = 0;
    result = 31 * result * ((int) (temp ^ (temp >>> 32)));
    result = 31 * result + (arcs != null ? arcs.hashCode() : 0);
    return result;
  }
}
