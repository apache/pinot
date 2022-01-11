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

  public static MutableFSTImpl emptyWithCopyOfSymbols(MutableFSTImpl mutableFSTImpl) {
    MutableFSTImpl copy = new MutableFSTImpl(mutableFSTImpl.states, mutableFSTImpl.outputSymbols);

    if (mutableFSTImpl.isUsingStateSymbols()) {
      copy.useStateSymbols(new MutableSymbolTable(mutableFSTImpl.getStateSymbols()));
    }

    return copy;
  }

  private final ArrayList<MutableState> states;
  private MutableState start;
  private WriteableSymbolTable outputSymbols;
  private MutableSymbolTable stateSymbols;

  public MutableFSTImpl() {
    this(new MutableSymbolTable());
  }

  /**
   * Constructs a new MutableFst with the given mutable symbol table; NOTE that these
   * symbol tables are being GIVEN to own by this MutableFst (transfer of ownership); tables should
   * not be SHARED by FSTs so if you want to make copies -- then do it yourself or set the tables
   * after construction via one of the applicable methods
   * @param outputSymbolsToOwn
   */
  public MutableFSTImpl(WriteableSymbolTable outputSymbolsToOwn) {
    this(Lists.newArrayList(), outputSymbolsToOwn);
  }

  protected MutableFSTImpl(ArrayList<MutableState> states,
                       WriteableSymbolTable outputSymbols) {
    this.states = states;
    this.outputSymbols = outputSymbols;

    this.stateSymbols = new MutableSymbolTable();
    this.start = new MutableState(true);
  }

  @Nullable
  @Override
  public WriteableSymbolTable getStateSymbols() {
    return stateSymbols;
  }

  /**
   * This sets a state symbols table; this takes ownership of this so don't share symbol
   * tables
   */
  public void useStateSymbols(MutableSymbolTable stateSymbolsToOwn) {
    this.stateSymbols = stateSymbolsToOwn;
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
    return newStartState(null);
  }

  public MutableState newStartState(@Nullable String startStateSymbol) {
    checkArgument(start == null, "cant add more than one start state");
    MutableState newStart = newState(startStateSymbol);
    setStartState(newStart);
    return newStart;
  }

  /**
   * Get the number of states in the fst
   */
  @Override
  public int getStateCount() {
    return this.states.size();
  }

  @Override
  public MutableState getState(int index) {
    return states.get(index);
  }

  @Override
  public MutableState getState(String name) {
    Preconditions.checkState(stateSymbols != null, "cant ask by name if not using state symbols");
    return getState(stateSymbols.get(name));
  }

  /**
   * Adds a state to the fst
   *
   * @param state the state to be added
   */
  public MutableState addState(MutableState state) {
    return addState(state, null);
  }

  public int lookupOutputSymbol(String symbol) {
    return outputSymbols.get(symbol);
  }

  public MutableState addState(MutableState state, @Nullable String newStateSymbol) {

    this.states.add(state);
    if (stateSymbols != null) {
      Preconditions.checkNotNull(newStateSymbol, "if using symbol table for states everything must have "
                                                 + "a symbol");
      stateSymbols.put(newStateSymbol, -1);
    } else {
      Preconditions.checkState(newStateSymbol == null, "cant pass state name if not using symbol table");
    }
    return state;
  }

  public MutableState setState(int id, MutableState state) {
    throwIfSymbolTableMissingId(id);
    // they provided the id so index properly
    if (id >= this.states.size()) {
      this.states.ensureCapacity(id + 1);
      for (int i = states.size(); i <= id; i++) {
        this.states.add(null);
      }
    }
    Preconditions.checkState(this.states.get(id) == null, "cant write two states with ", id);
    this.states.set(id, state);
    return state;
  }

  public MutableState newState() {
    return newState(null);
  }

  public MutableState newState(@Nullable String newStateSymbol) {
    MutableState s = new MutableState();
    return addState(s, newStateSymbol);
  }

  public MutableState getOrNewState(String stateSymbol) {
    Preconditions.checkNotNull(stateSymbols, "cant use this without state symbols");
    if (stateSymbols.contains(stateSymbol)) {
      return getState(stateSymbol);
    }
    return newState(stateSymbol);
  }

  /**
   * Adds a new arc in the FST between startStateSymbol and endStateSymbol with inSymbol and outSymbol
   * and edge weight; if the state symbols or in/out symbols dont exist then they will be added
   * @param startStateSymbol
   * @param outSymbol
   * @param endStateSymbol
   * @return
   */
  public MutableArc addArc(String startStateSymbol, int outSymbol, String endStateSymbol) {
    Preconditions.checkNotNull(stateSymbols, "cant use this without state symbols; call useStateSymbols()");
    return addArc(
        getOrNewState(startStateSymbol),
        outSymbol,
        getOrNewState(endStateSymbol)
    );
  }

  public MutableArc addArc(MutableState startState, int outputSymbol, MutableState endState) {
    MutableArc newArc = new MutableArc(outputSymbol,
                                        endState);
    startState.addArc(newArc);
    endState.addIncomingState(startState);
    return newArc;
  }

  @Override
  public WriteableSymbolTable getOutputSymbols() {
    return outputSymbols;
  }

  @Override
  public boolean isUsingStateSymbols() {
    return stateSymbols != null;
  }

  @Override
  public void throwIfInvalid() {
    Preconditions.checkNotNull(start, "must have a start state");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Fst(start=").append(start).append(", osyms=").append(
        outputSymbols);
    for (State s : states) {
      sb.append("  ").append(s).append("\n");
      int numArcs = s.getArcCount();
      for (int j = 0; j < numArcs; j++) {
        Arc a = s.getArc(j);
        sb.append("    ").append(a).append("\n");
      }
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

  private void throwIfSymbolTableMissingId(int id) {
    if (stateSymbols != null && !stateSymbols.invert().containsKey(id)) {
      throw new IllegalArgumentException("If you're using a state symbol table then every state "
                                         + "must be in the state symbol table");
    }
  }

  @Override
  public boolean equals(Object o) {
    return MutableFSTUtils.fstEquals(this, o);
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + (states != null ? states.hashCode() : 0);
    result = 31 * result + (start != null ? start.hashCode() : 0);
    result = 31 * result + (outputSymbols != null ? outputSymbols.hashCode() : 0);
    return result;
  }
}
