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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils.MutableFSTUtils;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A mutable finite state transducer implementation that allows you to build WFSTs via the API.
 * This is not thread safe; convert to an ImmutableFst if you need to share across threads.
 */
public class MutableFSTImpl implements MutableFST {
  private MutableState start;

  public MutableFSTImpl() {
    this.start = new MutableState(true);
  }

  /**
   * Get the initial states
   */
  @Override
  public MutableState getStartState() {
    return start;
  }

  /**
   * Set the initial state
   *
   * @param start the initial state
   */
  @Override
  public void setStartState(MutableState start) {
    if (this.start != null) {
      throw new IllegalStateException("Cannot override a start state");
    }

    this.start = start;
  }

  public MutableState newStartState() {
    return newStartState();
  }

  public MutableArc addArc(MutableState startState, int outputSymbol, MutableState endState) {
    MutableArc newArc = new MutableArc(outputSymbol,
                                        endState);
    startState.addArc(newArc);
    endState.addIncomingState(startState);
    return newArc;
  }

  @Override
  public void throwIfInvalid() {
    Preconditions.checkNotNull(start, "must have a start state");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Fst(start=").append(start).append(")");
    List<MutableArc> arcs = start.getArcs();

    for (MutableArc arc : arcs) {
      sb.append("  ").append(arc.toString()).append("\n");
    }
    return sb.toString();
  }

  @Override
  public void addPath(String word, int outputSymbol) {
    MutableState state = getStartState();

    if (state == null) {
      throw new IllegalStateException("Start state cannot be null");
    }

    List<MutableArc> arcs = state.getArcs();

    boolean isFound = false;

    for (MutableArc arc : arcs) {
      if (arc.getNextState().getLabel() == word.charAt(0)) {
        state = arc.getNextState();
        isFound = true;
        break;
      }
    }

    int foundPos = -1;

    if (isFound) {
      Pair<MutableState, Integer> pair = findPointOfDiversion(state, word, 0);

      if (pair == null) {
        // Word already exists
        return;
      }

      foundPos = pair.getRight();
      state = pair.getLeft();
    }

    for (int i = foundPos + 1; i < word.length(); i++) {
      MutableState nextState = new MutableState();

      nextState.setLabel(word.charAt(i));

      int currentOutputSymbol = -1;

      if (i == word.length() - 1) {
        currentOutputSymbol = outputSymbol;
      }

      MutableArc mutableArc = new MutableArc(currentOutputSymbol, nextState);
      state.addArc(mutableArc);

      state = nextState;
    }

    state.setIsTerminal(true);
  }

  private Pair<MutableState, Integer> findPointOfDiversion(MutableState mutableState,
      String word, int currentPos) {
    if (currentPos == word.length() - 1) {
      return null;
    }

    if (mutableState.getLabel() != word.charAt(currentPos)) {
      throw new IllegalStateException("Current state needs to be part of word path");
    }

    List<MutableArc> arcs = mutableState.getArcs();

    for (MutableArc arc : arcs) {
      if (arc.getNextState().getLabel() == word.charAt(currentPos + 1)) {
        return findPointOfDiversion(arc.getNextState(), word, currentPos + 1);
      }
    }

    return Pair.of(mutableState, currentPos);
  }

  static <T> void compactNulls(ArrayList<T> list) {
    int nextGood = 0;
    for (int i = 0; i < list.size(); i++) {
      T ss = list.get(i);
      if (ss != null) {
        if (i != nextGood) {
          list.set(nextGood, ss);
        }
        nextGood += 1;
      }
    }
    // trim the end
    while (list.size() > nextGood) {
      list.remove(list.size() - 1);
    }
  }

  @Override
  public boolean equals(Object o) {
    return MutableFSTUtils.fstEquals(this, o);
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + (start != null ? start.hashCode() : 0);
    return result;
  }
}
