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
package org.apache.pinot.query.runtime.operator.match;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.runtime.operator.match.PatternNfa.Transition;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;


/// Runs a [PatternNfa] over the rows of one partition and reports the SQL:2016 preferred match starting at a
/// given row.
///
/// ## First match found is the preferred match
///
/// The search is a depth first exploration that always takes the first not-yet-tried transition of the current state.
/// [PatternToNfaCompiler] orders the transitions of every state by SQL:2016 preference, so this enumeration is
/// exactly the preferment order and [#match] can return as soon as it reaches the accepting state. No candidate
/// match is ever scored or compared against another.
///
/// ## Only choice points are retained
///
/// Deterministic states are followed directly rather than pushed onto the search stack. The stack contains only
/// states with more than one outgoing transition, together with a checkpoint into a separate primitive counter-undo
/// log. Backtracking truncates the classifier tape to the choice point's row and restores that log. For a linear
/// `A+` match this retains one compact choice point per row rather than three full frames per row. The combined
/// primitive-array payload is hard-capped so raising the transition-step option cannot permit unbounded retained
/// backtracking state.
///
/// ## Running semantics come for free
///
/// A candidate row is pushed onto the tape *before* its DEFINE predicate is evaluated, so the predicate sees the
/// candidate as the current row and every navigation resolves against the rows matched so far. That is precisely the
/// SQL:2016 running semantics of DEFINE. If the predicate fails, the row is popped again.
///
/// Not thread safe: one instance owns one tape and one search stack.
public class PartitionMatcher {
  /// Returned by [#match] when no match starts at the requested row.
  public static final int NO_MATCH = -1;

  private static final int NO_POS = -1;
  private static final int INITIAL_STACK_CAPACITY = 64;
  private static final int TERMINATION_CHECK_INTERVAL = 1024;
  private static final long MAX_RETAINED_BACKTRACKING_BYTES = 64L * 1024 * 1024;
  private static final Runnable NO_OP_TERMINATION_CHECKER = () -> { };

  private final PatternNfa _nfa;
  /// DEFINE predicate per symbol ordinal; `null` means the variable matches every row, per SQL:2016.
  private final MatchExpression[] _definitions;
  private final MatchTape _tape;
  private final long _maxStepsPerMatchAttempt;
  private final long _maxRetainedBacktrackingBytes;
  private final Runnable _terminationChecker;
  private final int[] _counters;
  private final int[] _loopStartPos;

  private int[] _choiceState = new int[INITIAL_STACK_CAPACITY];
  private int[] _choicePos = new int[INITIAL_STACK_CAPACITY];
  private int[] _choiceNextTransition = new int[INITIAL_STACK_CAPACITY];
  private int[] _choiceCounterUndoSize = new int[INITIAL_STACK_CAPACITY];
  private int _choiceStackSize;

  private int[] _undoCounter = new int[INITIAL_STACK_CAPACITY];
  private int[] _undoCount = new int[INITIAL_STACK_CAPACITY];
  private int[] _undoLoopStart = new int[INITIAL_STACK_CAPACITY];
  private int _counterUndoSize;

  private int _state;
  private int _pos;
  private int _partitionSize;
  private int _matchStartPos;
  private long _steps;

  public PartitionMatcher(PatternNfa nfa, List<PatternSymbol> patternSymbols, DataSchema inputSchema,
      long maxStepsPerMatchAttempt) {
    this(nfa, patternSymbols, inputSchema, maxStepsPerMatchAttempt, NO_OP_TERMINATION_CHECKER,
        MAX_RETAINED_BACKTRACKING_BYTES);
  }

  public PartitionMatcher(PatternNfa nfa, List<PatternSymbol> patternSymbols, DataSchema inputSchema,
      long maxStepsPerMatchAttempt, Runnable terminationChecker) {
    this(nfa, patternSymbols, inputSchema, maxStepsPerMatchAttempt, terminationChecker,
        MAX_RETAINED_BACKTRACKING_BYTES);
  }

  @VisibleForTesting
  PartitionMatcher(PatternNfa nfa, List<PatternSymbol> patternSymbols, DataSchema inputSchema,
      long maxStepsPerMatchAttempt, Runnable terminationChecker, long maxRetainedBacktrackingBytes) {
    _nfa = nfa;
    _tape = new MatchTape(patternSymbols);
    _maxStepsPerMatchAttempt = maxStepsPerMatchAttempt;
    if (maxRetainedBacktrackingBytes < getRetainedBacktrackingBytes()) {
      throw new IllegalArgumentException("The retained backtracking byte limit must fit the initial matcher state");
    }
    _maxRetainedBacktrackingBytes = maxRetainedBacktrackingBytes;
    _terminationChecker = Objects.requireNonNull(terminationChecker, "terminationChecker");
    _definitions = new MatchExpression[patternSymbols.size()];
    for (int i = 0; i < _definitions.length; i++) {
      // A pattern variable that appears in PATTERN without a DEFINE entry matches every row, per SQL:2016.
      _definitions[i] = patternSymbols.get(i).getDefinition() == null ? null
          : MatchExpression.compile(patternSymbols.get(i).getDefinition(), inputSchema);
    }
    _counters = new int[nfa.getNumCounters()];
    _loopStartPos = new int[nfa.getNumCounters()];
  }

  /// The classifier tape. After a successful [#match] it describes that match and stays valid until the next
  /// call, so MEASURES are evaluated through it with final semantics.
  public MatchTape getTape() {
    return _tape;
  }

  /// Releases the partition retained by the classifier tape after its final measures have been evaluated.
  public void releasePartition() {
    _tape.reset(List.of(), 0, 0);
    _choiceStackSize = 0;
    _counterUndoSize = 0;
  }

  /// Finds the preferred match that starts exactly at `startPos`.
  ///
  /// @param matchNumber the value `MATCH_NUMBER()` reports; SQL:2016 assigns it before the match is known to
  ///        succeed, so it is passed in rather than derived
  /// @return the partition index one past the last row of the match, which equals `startPos` for an empty match,
  ///         or [#NO_MATCH] if no match starts here
  /// @throws org.apache.pinot.spi.exception.QueryException if the attempt exceeds the configured step budget. It
  ///         throws rather than giving up, because giving up would silently drop matches.
  public int match(List<Object[]> partitionRows, int startPos, long matchNumber) {
    _tape.reset(partitionRows, startPos, matchNumber);
    Arrays.fill(_counters, 0);
    Arrays.fill(_loopStartPos, NO_POS);
    _choiceStackSize = 0;
    _counterUndoSize = 0;
    _state = _nfa.getStartState();
    _pos = startPos;
    _partitionSize = partitionRows.size();
    _matchStartPos = startPos;
    _steps = 0;

    int acceptState = _nfa.getAcceptState();
    while (true) {
      if (_state == acceptState) {
        return _pos;
      }

      List<Transition> transitions = _nfa.getState(_state).getTransitions();
      if (transitions.size() > 1) {
        pushChoice(_state, _pos);
        if (takeNextChoice()) {
          continue;
        }
      } else if (transitions.size() == 1) {
        recordStep(_pos);
        if (tryApply(transitions.get(0), _pos)) {
          continue;
        }
      }

      if (takeNextChoice()) {
        continue;
      }
      restoreTo(startPos, 0);
      return NO_MATCH;
    }
  }

  /// Applies `transition` at `pos`, updating the current state when its guard holds.
  ///
  /// @return whether the transition was taken; on success [#_state] and [#_pos] hold its target
  private boolean tryApply(Transition transition, int pos) {
    int target = transition.getTarget();
    switch (transition.getKind()) {
      case MATCH: {
        if (pos >= _partitionSize) {
          return false;
        }
        int symbolOrdinal = transition.getOperand();
        // Push before evaluating so the predicate sees the candidate row as the current row.
        _tape.push(symbolOrdinal);
        MatchExpression definition = _definitions[symbolOrdinal];
        if (definition != null && !definition.test(_tape)) {
          _tape.pop();
          return false;
        }
        return advanceTo(target, pos + 1);
      }
      case EPSILON:
        return advanceTo(target, pos);
      case START_LOOP: {
        int counterId = transition.getOperand();
        pushCounterUndo(counterId);
        _counters[counterId] = 0;
        _loopStartPos[counterId] = NO_POS;
        return advanceTo(target, pos);
      }
      case REPEAT: {
        int counterId = transition.getOperand();
        int maxRepeat = transition.getBound();
        if (maxRepeat != PatternNfa.UNBOUNDED && _counters[counterId] >= maxRepeat) {
          return false;
        }
        // Empty cycle guard: the previous iteration of this quantifier consumed no row, so another one never will.
        if (_loopStartPos[counterId] == pos) {
          return false;
        }
        pushCounterUndo(counterId);
        _counters[counterId]++;
        _loopStartPos[counterId] = pos;
        return advanceTo(target, pos);
      }
      case EXIT_LOOP: {
        int counterId = transition.getOperand();
        // An empty iteration may be repeated vacuously any number of times, so it satisfies any minimum.
        if (_counters[counterId] < transition.getBound() && _loopStartPos[counterId] != pos) {
          return false;
        }
        return advanceTo(target, pos);
      }
      case ANCHOR_START:
        if (pos != 0) {
          return false;
        }
        return advanceTo(target, pos);
      case ANCHOR_END:
        if (pos != _partitionSize) {
          return false;
        }
        return advanceTo(target, pos);
      default:
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "Unsupported MATCH_RECOGNIZE pattern transition: " + transition.getKind());
    }
  }

  private boolean advanceTo(int state, int pos) {
    _state = state;
    _pos = pos;
    return true;
  }

  private void pushChoice(int state, int pos) {
    if (_choiceStackSize == _choiceState.length) {
      growChoiceStack();
    }
    _choiceState[_choiceStackSize] = state;
    _choicePos[_choiceStackSize] = pos;
    _choiceNextTransition[_choiceStackSize] = 0;
    _choiceCounterUndoSize[_choiceStackSize] = _counterUndoSize;
    _choiceStackSize++;
  }

  /// Restores successive choice points and takes their next valid transition, in preference order.
  private boolean takeNextChoice() {
    while (_choiceStackSize > 0) {
      int top = _choiceStackSize - 1;
      int choicePos = _choicePos[top];
      int undoSize = _choiceCounterUndoSize[top];
      restoreTo(choicePos, undoSize);
      List<Transition> transitions = _nfa.getState(_choiceState[top]).getTransitions();
      while (_choiceNextTransition[top] < transitions.size()) {
        Transition transition = transitions.get(_choiceNextTransition[top]++);
        recordStep(choicePos);
        if (tryApply(transition, choicePos)) {
          return true;
        }
        restoreTo(choicePos, undoSize);
      }
      _choiceStackSize--;
    }
    return false;
  }

  private void pushCounterUndo(int counterId) {
    if (_counterUndoSize == _undoCounter.length) {
      growCounterUndoLog();
    }
    _undoCounter[_counterUndoSize] = counterId;
    _undoCount[_counterUndoSize] = _counters[counterId];
    _undoLoopStart[_counterUndoSize] = _loopStartPos[counterId];
    _counterUndoSize++;
  }

  private void restoreTo(int pos, int undoSize) {
    while (_tape.getEndPos() > pos) {
      _tape.pop();
    }
    while (_counterUndoSize > undoSize) {
      int undo = --_counterUndoSize;
      int counterId = _undoCounter[undo];
      _counters[counterId] = _undoCount[undo];
      _loopStartPos[counterId] = _undoLoopStart[undo];
    }
  }

  private void recordStep(int pos) {
    if (++_steps > _maxStepsPerMatchAttempt) {
      // The step count includes the transitions that make linear progress, so a long match over a large partition can
      // hit this without any backtracking at all. The partition and consumed row counts distinguish the two cases.
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
          "MATCH_RECOGNIZE exceeded the maximum of " + _maxStepsPerMatchAttempt
              + " pattern matching steps for the match attempt starting at row " + _matchStartPos + " of a "
              + _partitionSize + "-row partition (" + (pos - _matchStartPos) + " rows consumed so far). Raise the '"
              + QueryOptionKey.MAX_STEPS_PER_MATCH_ATTEMPT + "' query option, or if the row count consumed is far "
              + "below the step count, make the PATTERN less ambiguous and tighten the DEFINE predicates.");
    }
    if ((_steps & (TERMINATION_CHECK_INTERVAL - 1)) == 0) {
      _terminationChecker.run();
    }
  }

  private void growChoiceStack() {
    int capacity = _choiceState.length * 2;
    checkRetainedBacktrackingCapacity(capacity, _undoCounter.length);
    _choiceState = Arrays.copyOf(_choiceState, capacity);
    _choicePos = Arrays.copyOf(_choicePos, capacity);
    _choiceNextTransition = Arrays.copyOf(_choiceNextTransition, capacity);
    _choiceCounterUndoSize = Arrays.copyOf(_choiceCounterUndoSize, capacity);
  }

  private void growCounterUndoLog() {
    int capacity = _undoCounter.length * 2;
    checkRetainedBacktrackingCapacity(_choiceState.length, capacity);
    _undoCounter = Arrays.copyOf(_undoCounter, capacity);
    _undoCount = Arrays.copyOf(_undoCount, capacity);
    _undoLoopStart = Arrays.copyOf(_undoLoopStart, capacity);
  }

  @VisibleForTesting
  long getRetainedBacktrackingBytes() {
    return retainedBacktrackingBytes(_choiceState.length, _undoCounter.length);
  }

  private void checkRetainedBacktrackingCapacity(int choiceCapacity, int undoCapacity) {
    long retainedBytes = retainedBacktrackingBytes(choiceCapacity, undoCapacity);
    if (retainedBytes > _maxRetainedBacktrackingBytes) {
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
          "MATCH_RECOGNIZE exceeded the hard limit of " + _maxRetainedBacktrackingBytes
              + " bytes for retained pattern backtracking state. Reduce the partition size or simplify the PATTERN.");
    }
  }

  private static long retainedBacktrackingBytes(int choiceCapacity, int undoCapacity) {
    return (long) Integer.BYTES * (4L * choiceCapacity + 3L * undoCapacity);
  }

  /// The DEFINE predicate compiled for `symbolOrdinal`, or `null` if the variable matches every row.
  @Nullable
  MatchExpression getDefinition(int symbolOrdinal) {
    return _definitions[symbolOrdinal];
  }
}
