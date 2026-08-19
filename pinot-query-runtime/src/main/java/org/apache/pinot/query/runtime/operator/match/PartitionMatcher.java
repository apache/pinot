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

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.runtime.operator.match.PatternNfa.Transition;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * Runs a {@link PatternNfa} over the rows of one partition and reports the SQL:2016 preferred match starting at a
 * given row.
 *
 * <h2>First match found is the preferred match</h2>
 * The search is a depth first exploration that always takes the first not-yet-tried transition of the current state.
 * {@link PatternToNfaCompiler} orders the transitions of every state by SQL:2016 preference, so this enumeration is
 * exactly the preferment order and {@link #match} can return as soon as it reaches the accepting state. No candidate
 * match is ever scored or compared against another.
 *
 * <h2>Backtracking is O(1) per step</h2>
 * The search stack is held in parallel primitive arrays rather than objects, and each frame records the single undo
 * it needs: whether it pushed a row onto the {@link MatchTape}, and which counter register it changed together with
 * that register's previous value. Popping a frame therefore costs a couple of array writes; nothing is copied or
 * rebuilt, and the classifier tape stays append only.
 *
 * <h2>Running semantics come for free</h2>
 * A candidate row is pushed onto the tape <i>before</i> its DEFINE predicate is evaluated, so the predicate sees the
 * candidate as the current row and every navigation resolves against the rows matched so far. That is precisely the
 * SQL:2016 running semantics of DEFINE. If the predicate fails, the row is popped again.
 *
 * <p>Not thread safe: one instance owns one tape and one search stack.
 */
public class PartitionMatcher {
  /** Returned by {@link #match} when no match starts at the requested row. */
  public static final int NO_MATCH = -1;

  private static final int NO_COUNTER = -1;
  private static final int NO_POS = -1;
  private static final int INITIAL_STACK_CAPACITY = 64;

  private final PatternNfa _nfa;
  /** DEFINE predicate per symbol ordinal; {@code null} means the variable matches every row, per SQL:2016. */
  private final MatchExpression[] _definitions;
  private final MatchTape _tape;
  private final long _maxStepsPerMatchAttempt;
  private final int[] _counters;
  private final int[] _loopStartPos;

  private int[] _frameState = new int[INITIAL_STACK_CAPACITY];
  private int[] _framePos = new int[INITIAL_STACK_CAPACITY];
  private int[] _frameNextTransition = new int[INITIAL_STACK_CAPACITY];
  private int[] _frameUndoCounter = new int[INITIAL_STACK_CAPACITY];
  private int[] _frameUndoCount = new int[INITIAL_STACK_CAPACITY];
  private int[] _frameUndoLoopStart = new int[INITIAL_STACK_CAPACITY];
  private boolean[] _frameUndoTapePush = new boolean[INITIAL_STACK_CAPACITY];
  private int _stackSize;

  public PartitionMatcher(PatternNfa nfa, List<PatternSymbol> patternSymbols, DataSchema inputSchema,
      long maxStepsPerMatchAttempt) {
    _nfa = nfa;
    _tape = new MatchTape(patternSymbols);
    _maxStepsPerMatchAttempt = maxStepsPerMatchAttempt;
    _definitions = new MatchExpression[patternSymbols.size()];
    for (int i = 0; i < _definitions.length; i++) {
      // A pattern variable that appears in PATTERN without a DEFINE entry matches every row, per SQL:2016.
      _definitions[i] = patternSymbols.get(i).getDefinition() == null ? null
          : MatchExpression.compile(patternSymbols.get(i).getDefinition(), inputSchema);
    }
    _counters = new int[nfa.getNumCounters()];
    _loopStartPos = new int[nfa.getNumCounters()];
  }

  /**
   * The classifier tape. After a successful {@link #match} it describes that match and stays valid until the next
   * call, so MEASURES are evaluated through it with final semantics.
   */
  public MatchTape getTape() {
    return _tape;
  }

  /**
   * Finds the preferred match that starts exactly at {@code startPos}.
   *
   * @param matchNumber the value {@code MATCH_NUMBER()} reports; SQL:2016 assigns it before the match is known to
   *        succeed, so it is passed in rather than derived
   * @return the partition index one past the last row of the match, which equals {@code startPos} for an empty match,
   *         or {@link #NO_MATCH} if no match starts here
   * @throws org.apache.pinot.spi.exception.QueryException if the attempt exceeds the configured step budget. It
   *         throws rather than giving up, because giving up would silently drop matches.
   */
  public int match(List<Object[]> partitionRows, int startPos, long matchNumber) {
    _tape.reset(partitionRows, startPos, matchNumber);
    Arrays.fill(_counters, 0);
    Arrays.fill(_loopStartPos, NO_POS);
    _stackSize = 0;
    pushFrame(_nfa.getStartState(), startPos, NO_COUNTER, 0, 0, false);

    int partitionSize = partitionRows.size();
    int acceptState = _nfa.getAcceptState();
    long steps = 0;
    while (_stackSize > 0) {
      int top = _stackSize - 1;
      if (_frameState[top] == acceptState) {
        return _framePos[top];
      }
      List<Transition> transitions = _nfa.getState(_frameState[top]).getTransitions();
      boolean advanced = false;
      while (_frameNextTransition[top] < transitions.size()) {
        Transition transition = transitions.get(_frameNextTransition[top]++);
        if (++steps > _maxStepsPerMatchAttempt) {
          // The step count includes the transitions that make linear progress, so a long match over a large
          // partition can hit this without any backtracking at all. Reporting the partition size and the number of
          // rows consumed so far is what makes the two distinguishable, and the tuning remedy comes first because a
          // linear blowup has no ambiguity to remove.
          throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
              "MATCH_RECOGNIZE exceeded the maximum of " + _maxStepsPerMatchAttempt
                  + " pattern matching steps for the match attempt starting at row " + startPos + " of a "
                  + partitionSize + "-row partition (" + (_framePos[top] - startPos) + " rows consumed so far). "
                  + "Raise the '" + MatchLimits.MAX_STEPS_PER_MATCH_ATTEMPT + "' query option, or if the row count "
                  + "consumed is far below the step count, make the PATTERN less ambiguous and tighten the DEFINE "
                  + "predicates.");
        }
        if (tryApply(transition, top, partitionSize)) {
          advanced = true;
          break;
        }
      }
      if (!advanced) {
        popFrame();
      }
    }
    return NO_MATCH;
  }

  /**
   * Applies {@code transition} to the frame at {@code parent}, pushing the resulting frame if its guard holds.
   *
   * @return whether the transition was taken
   */
  private boolean tryApply(Transition transition, int parent, int partitionSize) {
    int pos = _framePos[parent];
    int target = transition.getTarget();
    switch (transition.getKind()) {
      case MATCH: {
        if (pos >= partitionSize) {
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
        pushFrame(target, pos + 1, NO_COUNTER, 0, 0, true);
        return true;
      }
      case EPSILON:
        pushFrame(target, pos, NO_COUNTER, 0, 0, false);
        return true;
      case START_LOOP: {
        int counterId = transition.getOperand();
        pushFrame(target, pos, counterId, _counters[counterId], _loopStartPos[counterId], false);
        _counters[counterId] = 0;
        _loopStartPos[counterId] = NO_POS;
        return true;
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
        pushFrame(target, pos, counterId, _counters[counterId], _loopStartPos[counterId], false);
        _counters[counterId]++;
        _loopStartPos[counterId] = pos;
        return true;
      }
      case EXIT_LOOP: {
        int counterId = transition.getOperand();
        // An empty iteration may be repeated vacuously any number of times, so it satisfies any minimum.
        if (_counters[counterId] < transition.getBound() && _loopStartPos[counterId] != pos) {
          return false;
        }
        pushFrame(target, pos, NO_COUNTER, 0, 0, false);
        return true;
      }
      case ANCHOR_START:
        if (pos != 0) {
          return false;
        }
        pushFrame(target, pos, NO_COUNTER, 0, 0, false);
        return true;
      case ANCHOR_END:
        if (pos != partitionSize) {
          return false;
        }
        pushFrame(target, pos, NO_COUNTER, 0, 0, false);
        return true;
      default:
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "Unsupported MATCH_RECOGNIZE pattern transition: " + transition.getKind());
    }
  }

  /**
   * Pushes a frame together with the undo of the transition that created it: at most one tape row and at most one
   * counter register change.
   */
  private void pushFrame(int state, int pos, int undoCounter, int undoCount, int undoLoopStart,
      boolean undoTapePush) {
    if (_stackSize == _frameState.length) {
      growStack();
    }
    _frameState[_stackSize] = state;
    _framePos[_stackSize] = pos;
    _frameNextTransition[_stackSize] = 0;
    _frameUndoCounter[_stackSize] = undoCounter;
    _frameUndoCount[_stackSize] = undoCount;
    _frameUndoLoopStart[_stackSize] = undoLoopStart;
    _frameUndoTapePush[_stackSize] = undoTapePush;
    _stackSize++;
  }

  private void popFrame() {
    int top = --_stackSize;
    if (_frameUndoTapePush[top]) {
      _tape.pop();
    }
    int undoCounter = _frameUndoCounter[top];
    if (undoCounter != NO_COUNTER) {
      _counters[undoCounter] = _frameUndoCount[top];
      _loopStartPos[undoCounter] = _frameUndoLoopStart[top];
    }
  }

  private void growStack() {
    int capacity = _frameState.length * 2;
    _frameState = Arrays.copyOf(_frameState, capacity);
    _framePos = Arrays.copyOf(_framePos, capacity);
    _frameNextTransition = Arrays.copyOf(_frameNextTransition, capacity);
    _frameUndoCounter = Arrays.copyOf(_frameUndoCounter, capacity);
    _frameUndoCount = Arrays.copyOf(_frameUndoCount, capacity);
    _frameUndoLoopStart = Arrays.copyOf(_frameUndoLoopStart, capacity);
    _frameUndoTapePush = Arrays.copyOf(_frameUndoTapePush, capacity);
  }

  /**
   * The DEFINE predicate compiled for {@code symbolOrdinal}, or {@code null} if the variable matches every row.
   */
  @Nullable
  MatchExpression getDefinition(int symbolOrdinal) {
    return _definitions[symbolOrdinal];
  }
}
