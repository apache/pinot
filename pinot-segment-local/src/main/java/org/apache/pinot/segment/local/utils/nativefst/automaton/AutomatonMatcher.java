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

import java.util.regex.MatchResult;

/**
 * A tool that performs match operations on a given character sequence using
 * a compiled automaton.
 *
 * @see RunAutomaton#newMatcher(CharSequence)
 * @see RunAutomaton#newMatcher(CharSequence, int, int)
 */
public class AutomatonMatcher implements MatchResult {

	private final RunAutomaton _automaton;
	private final CharSequence _chars;

	private int _matchStart = -1;

	private int _matchEnd = -1;

	AutomatonMatcher(final CharSequence chars, final RunAutomaton automaton) {
		this._chars = chars;
		this._automaton = automaton;
	}

	/**
	 * Find the next matching subsequence of the input.
	 * <br>
	 * This also updates the values for the {@code start}, {@code end}, and
	 * {@code group} methods.
	 *
	 * @return {@code true} if there is a matching subsequence.
	 */
	public boolean find() {
		int begin;
		switch(getMatchStart()) {
			case -2:
			return false;
			case -1:
			begin = 0;
				break;
			default:
			begin = getMatchEnd();
				// This occurs when a previous find() call matched the empty string.
				// This can happen when the pattern is a* for example.
				if(begin == getMatchStart()) {
					begin += 1;
					if(begin > getChars().length()) {
						setMatch(-2, -2);
						return false;
					}
				}
		}

		int match_start;
		int match_end;
		if (_automaton.isAccept(_automaton.getInitialState())) {
			match_start = begin;
			match_end = begin;
		} else {
			match_start = -1;
			match_end = -1;
		}
		int l = getChars().length();
		while (begin < l) {
			int p = _automaton.getInitialState();
			for (int i = begin; i < l; i++) {
				final int new_state = _automaton.step(p, getChars().charAt(i));
				if (new_state == -1) {
				    break;
				} else if (_automaton.isAccept(new_state)) {
				    // found a match from begin to (i+1)
				    match_start = begin;
				    match_end=(i+1);
				}
				p = new_state;
			}
			if (match_start != -1) {
				setMatch(match_start, match_end);
				return true;
			}
			begin += 1;
		}
		if (match_start != -1) {
			setMatch(match_start, match_end);
			return true;
		} else {
			setMatch(-2, -2);
			return false;
		}
	}

	private void setMatch(final int matchStart, final int matchEnd) throws IllegalArgumentException {
		if (matchStart > matchEnd) {
			throw new IllegalArgumentException("Start must be less than or equal to end: " + matchStart + ", " + matchEnd);
		}
		this._matchStart = matchStart;
		this._matchEnd = matchEnd;
	}

	private int getMatchStart() {
		return _matchStart;
	}

	private int getMatchEnd() {
		return _matchEnd;
	}

	private CharSequence getChars() {
		return _chars;
	}

	/**
	 * Returns the offset after the last character matched.
	 *
	 * @return The offset after the last character matched.
	 * @throws IllegalStateException if there has not been a match attempt or
	 *  if the last attempt yielded no results.
	 */
	public int end() throws IllegalStateException {
		matchGood();
		return _matchEnd;
	}

	/**
	 * Returns the offset after the last character matched of the specified
	 * capturing group.
	 * <br>
	 * Note that because the automaton does not support capturing groups the
	 * only valid group is 0 (the entire match).
	 *
	 * @param group the desired capturing group.
	 * @return The offset after the last character matched of the specified
	 *  capturing group.
	 * @throws IllegalStateException if there has not been a match attempt or
	 *  if the last attempt yielded no results.
	 * @throws IndexOutOfBoundsException if the specified capturing group does
	 *  not exist in the underlying automaton.
	 */
	public int end(final int group) throws IndexOutOfBoundsException, IllegalStateException {
		onlyZero(group);
		return end();
	}

	/**
	 * Returns the subsequence of the input found by the previous match.
	 *
	 * @return The subsequence of the input found by the previous match.
	 * @throws IllegalStateException if there has not been a match attempt or
	 *  if the last attempt yielded no results.
	 */
	public String group() throws IllegalStateException {
		matchGood();
		return _chars.subSequence(_matchStart, _matchEnd).toString();
	}

	/**
	 * Returns the subsequence of the input found by the specified capturing
	 * group during the previous match operation.
	 * <br>
	 * Note that because the automaton does not support capturing groups the
	 * only valid group is 0 (the entire match).
	 *
	 * @param group the desired capturing group.
	 * @return The subsequence of the input found by the specified capturing
	 *  group during the previous match operation the previous match. Or
	 *  {@code null} if the given group did match.
	 * @throws IllegalStateException if there has not been a match attempt or
	 *  if the last attempt yielded no results.
	 * @throws IndexOutOfBoundsException if the specified capturing group does
	 *  not exist in the underlying automaton.
	 */
	public String group(final int group) throws IndexOutOfBoundsException, IllegalStateException {
		onlyZero(group);
		return group();
	}

	/**
	 * Returns the number of capturing groups in the underlying automaton.
	 * <br>
	 * Note that because the automaton does not support capturing groups this
	 * method will always return 0.
	 *
	 * @return The number of capturing groups in the underlying automaton.
	 */
	public int groupCount() {
		return 0;
	}

	/**
	 * Returns the offset of the first character matched.
	 *
	 * @return The offset of the first character matched.
	 * @throws IllegalStateException if there has not been a match attempt or
	 *  if the last attempt yielded no results.
	 */
	public int start() throws IllegalStateException {
		matchGood();
		return _matchStart;
	}

	/**
	 * Returns the offset of the first character matched of the specified
	 * capturing group.
	 * <br>
	 * Note that because the automaton does not support capturing groups the
	 * only valid group is 0 (the entire match).
	 *
	 * @param group the desired capturing group.
	 * @return The offset of the first character matched of the specified
	 *  capturing group.
	 * @throws IllegalStateException if there has not been a match attempt or
	 *  if the last attempt yielded no results.
	 * @throws IndexOutOfBoundsException if the specified capturing group does
	 *  not exist in the underlying automaton.
	 */
	public int start(int group) throws IndexOutOfBoundsException, IllegalStateException {
		onlyZero(group);
		return start();
	}

	/** Helper method that requires the group argument to be 0. */
	private static void onlyZero(final int group) throws IndexOutOfBoundsException {
		if (group != 0) {
			throw new IndexOutOfBoundsException("The only group supported is 0.");
		}
	}

	/** Helper method to check that the last match attempt was valid. */
	private void matchGood() throws IllegalStateException {
		if ((_matchStart < 0) || (_matchEnd < 0)) {
			throw new IllegalStateException("There was no available match.");
		}
	}
}
