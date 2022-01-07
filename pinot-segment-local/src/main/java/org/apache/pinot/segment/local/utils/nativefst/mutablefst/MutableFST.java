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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils.FstUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;


/**
 * A mutable finite state transducer implementation that allows you to build WFSTs via the API.
 * This is not thread safe; convert to an ImmutableFst if you need to share across threads.
 */
public class MutableFST implements ChangeableFST {

  public static MutableFST emptyWithCopyOfSymbols(MutableFST mutableFST) {
    MutableFST copy = new MutableFST(mutableFST.states, mutableFST.inputSymbols, mutableFST.outputSymbols);

    if (mutableFST.isUsingStateSymbols()) {
      copy.useStateSymbols(new MutableSymbolTable(mutableFST.getStateSymbols()));
    }

    return copy;
  }

  /**
   * Make a deep copy of the given FST. The symbol tables are "effectively deep copied" meaning that they will
   * take advantage of any immutable tables (using the UnionSymbolTable) to avoid doing a large deep copy.
   * @param mutableFST
   * @return
   */
  public static MutableFST copyFrom(MutableFST mutableFST) {
    MutableFST copy = emptyWithCopyOfSymbols(mutableFST);
    // build up states
    for (int i = 0; i < mutableFST.getStateCount(); i++) {
      State source = mutableFST.getState(i);
      MutableState target = new MutableState(source.getArcCount());
      copy.setState(i, target);
    }
    // build arcs now that we have target state refs
    for (int i = 0; i < mutableFST.getStateCount(); i++) {
      State source = mutableFST.getState(i);
      MutableState target = copy.getState(i);
      for (int j = 0; j < source.getArcCount(); j++) {
        Arc sarc = source.getArc(j);
        MutableState nextTargetState = copy.getState(sarc.getNextState().getId());
        MutableArc
            tarc = new MutableArc(sarc.getIlabel(), sarc.getOlabel(), nextTargetState);
        target.addArc(tarc);
      }
    }
    MutableState newStart = copy.getState(mutableFST.getStartState().getId());
    copy.setStart(newStart);
    return copy;
  }

  private final ArrayList<MutableState> states;
  private MutableState start;
  private WriteableSymbolTable inputSymbols;
  private WriteableSymbolTable outputSymbols;
  private MutableSymbolTable stateSymbols;

  public MutableFST() {
    this(new MutableSymbolTable(), new MutableSymbolTable());
  }

  /**
   * Constructor specifying the initial capacity of the states ArrayList (this is an optimization used in various
   * operations)
   *
   * @param numStates the initial capacity
   */
  public MutableFST(int numStates) {
    this(new ArrayList<MutableState>(numStates), new MutableSymbolTable(), new MutableSymbolTable());
  }

  /**
   * Constructs a new MutableFst with the given semiring and mutable symbol tables; NOTE that these
   * symbol tables are being GIVEN to own by this MutableFst (transfer of ownership); tables should
   * not be SHARED by FSTs so if you want to make copies -- then do it yourself or set the tables
   * after construction via one of the applicable methods
   * @param inputSymbolsToOwn
   * @param outputSymbolsToOwn
   */
  public MutableFST(WriteableSymbolTable inputSymbolsToOwn, WriteableSymbolTable outputSymbolsToOwn) {
    this(Lists.newArrayList(), inputSymbolsToOwn, outputSymbolsToOwn);
  }

  protected MutableFST(ArrayList<MutableState> states, WriteableSymbolTable inputSymbols,
                       WriteableSymbolTable outputSymbols) {
    this.states = states;
    this.inputSymbols = inputSymbols;
    this.outputSymbols = outputSymbols;

    this.stateSymbols = new MutableSymbolTable();
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
  public MutableState setStart(MutableState start) {
    checkArgument(start.getId() >= 0, "must set id before setting start");
    throwIfSymbolTableMissingId(start.getId());

    this.start = start;
    return start;
  }

  public MutableState newStartState() {
    return newStartState(null);
  }

  public MutableState newStartState(@Nullable String startStateSymbol) {
    checkArgument(start == null, "cant add more than one start state");
    MutableState newStart = newState(startStateSymbol);
    setStart(newStart);
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

  public int lookupInputSymbol(String symbol) {
    return inputSymbols.get(symbol);
  }

  public int lookupOutputSymbol(String symbol) {
    return outputSymbols.get(symbol);
  }

  public MutableState addState(MutableState state, @Nullable String newStateSymbol) {
    checkArgument(state.getId() == -1, "trying to add a state that already has id");
    this.states.add(state);
    state.id = states.size() - 1;
    if (stateSymbols != null) {
      Preconditions.checkNotNull(newStateSymbol, "if using symbol table for states everything must have "
                                                 + "a symbol");
      stateSymbols.put(newStateSymbol, state.id);
    } else {
      Preconditions.checkState(newStateSymbol == null, "cant pass state name if not using symbol table");
    }
    return state;
  }

  public MutableState setState(int id, MutableState state) {
    checkArgument(state.getId() == -1, "trying to add a state that already has id");
    state.setId(id);
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
   * @param inSymbol
   * @param outSymbol
   * @param endStateSymbol
   * @return
   */
  public MutableArc addArc(String startStateSymbol, String inSymbol, String outSymbol, String endStateSymbol) {
    Preconditions.checkNotNull(stateSymbols, "cant use this without state symbols; call useStateSymbols()");
    return addArc(
        getOrNewState(startStateSymbol),
        inSymbol,
        outSymbol,
        getOrNewState(endStateSymbol)
    );
  }

  public MutableArc addArc(String startStateSymbol, int inSymbolId, int outSymbolId, String endStateSymbol) {
    Preconditions.checkNotNull(stateSymbols, "cant use this without state symbols; call useStateSymbols()");
    return addArc(
        getOrNewState(startStateSymbol),
        inSymbolId,
        outSymbolId,
        getOrNewState(endStateSymbol)
    );
  }

  public MutableArc addArc(MutableState startState, String inSymbol, String outSymbol, MutableState endState) {
    return addArc(startState, inputSymbols.getOrAdd(inSymbol), outputSymbols.getOrAdd(outSymbol), endState);
  }

  public MutableArc addArc(MutableState startState, int inSymbolId, int outSymbolId, MutableState endState) {
    checkArgument(this.states.get(startState.getId()) == startState, "cant pass state that doesnt exist in fst");
    checkArgument(this.states.get(endState.getId()) == endState, "cant pass end state that doesnt exist in fst");
    MutableArc newArc = new MutableArc(inSymbolId,
                                        outSymbolId,
                                        endState);
    startState.addArc(newArc);
    endState.addIncomingState(startState);
    return newArc;
  }

  @Override
  public WriteableSymbolTable getInputSymbols() {
    return inputSymbols;
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
    sb.append("Fst(start=").append(start).append(", isyms=").append(inputSymbols).append(", osyms=").append(
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

  /**
   * Deletes the given states and remaps the existing state ids
   */
  public void deleteStates(Collection<MutableState> statesToDelete) {
    if (statesToDelete.isEmpty()) {
      return;
    }
    for (MutableState state : statesToDelete) {
      deleteState(state);
    }
    remapStateIds();
  }

  /**
   * Deletes a state;
   *
   * @param state the state to delete
   */
  private void deleteState(MutableState state) {
    if (state.getId() == this.start.getId()) {
      throw new IllegalArgumentException("Cannot delete start state.");
    }
    // we're going to "compact" all of the nulls out and remap state ids at the end
    this.states.set(state.getId(), null);
    if (isUsingStateSymbols()) {
      stateSymbols.remove(state.getId());
    }

    // this state won't be incoming to any of its arc's targets anymore
    for (MutableArc mutableArc : state.getArcs()) {
      mutableArc.getNextState().removeIncomingState(state);
    }

    // delete arc's with nextstate equal to stateid
    for (MutableState inState : state.getIncomingStates()) {
      Iterator<MutableArc> iter = inState.getArcs().iterator();
      while (iter.hasNext()) {
        MutableArc arc = iter.next();
        if (arc.getNextState() == state) {
          iter.remove();
        }
      }
    }
  }

  private void remapStateIds() {
    // clear all of the nulls out

    compactNulls(states);
    int numStates = states.size();
    ArrayList<IndexPair> toRemap = null;
    if (isUsingStateSymbols()) {
      toRemap = Lists.newArrayList();
    }
    for (int i = 0; i < numStates; i++) {
      MutableState mutableState = states.get(i);
      if (mutableState.id != i) {
        if (isUsingStateSymbols()) {
          toRemap.add(new IndexPair(mutableState.getId(), i));
        }
        mutableState.id = i;
      }
    }
    if (isUsingStateSymbols()) {
      stateSymbols.remapAll(toRemap);
      stateSymbols.trimIds();
    }
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

  public void throwIfAnyNullStates() {
    for (int i = 0; i < states.size(); i++) {
      if (states.get(i) == null) {
        throw new IllegalStateException("Cannot have a null state in an FST. State " + i);
      }
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
    return FstUtils.fstEquals(this, o);
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + (states != null ? states.hashCode() : 0);
    result = 31 * result + (start != null ? start.hashCode() : 0);
    result = 31 * result + (inputSymbols != null ? inputSymbols.hashCode() : 0);
    result = 31 * result + (outputSymbols != null ? outputSymbols.hashCode() : 0);
    return result;
  }
}
