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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Construction of basic automata.
 */
public final class BasicAutomata {
	
	private BasicAutomata() {}

	/** 
	 * Returns a new (deterministic) automaton with the empty language. 
	 */
	public static Automaton makeEmpty() {
		Automaton a = new Automaton();
		a._initial = new State();
		a._deterministic = true;
		return a;
	}
	
	/** 
	 * Returns a new (deterministic) automaton that accepts only the empty string. 
	 */
	public static Automaton makeEmptyString() {
		Automaton a = new Automaton();
		a._singleton = "";
		a._deterministic = true;
		return a;
	}
	
	/** 
	 * Returns a new (deterministic) automaton that accepts all strings. 
	 */
	public static Automaton makeAnyString()	{
		Automaton a = new Automaton();
		State s = new State();
		a._initial = s;
		s._accept = true;
		s._transitionSet.add(new Transition(Character.MIN_VALUE, Character.MAX_VALUE, s));
		a._deterministic = true;
		return a;
	}
	
	/** 
	 * Returns a new (deterministic) automaton that accepts any single character. 
	 */
	public static Automaton makeAnyChar() {
		return makeCharRange(Character.MIN_VALUE, Character.MAX_VALUE);
	}
	
	/** 
	 * Returns a new (deterministic) automaton that accepts a single character of the given value. 
	 */
	public static Automaton makeChar(char c) {
		Automaton a = new Automaton();
		a._singleton = Character.toString(c);
		a._deterministic = true;
		return a;
	}
	
	/** 
	 * Returns a new (deterministic) automaton that accepts a single char 
	 * whose value is in the given interval (including both end points). 
	 */
	public static Automaton makeCharRange(char min, char max) {
		if (min == max) {
			return makeChar(min);
		}
		Automaton a = new Automaton();
		State s1 = new State();
		State s2 = new State();
		a._initial = s1;
		s2._accept = true;
		if (min <= max) {
			s1._transitionSet.add(new Transition(min, max, s2));
		}
		a._deterministic = true;
		return a;
	}
	
	/** 
	 * Returns a new (deterministic) automaton that accepts a single character in the given set. 
	 */
	public static Automaton makeCharSet(String set) {
		if (set.length() == 1) {
			return makeChar(set.charAt(0));
		}
		Automaton a = new Automaton();
		State s1 = new State();
		State s2 = new State();
		a._initial = s1;
		s2._accept = true;
		for (int i = 0; i < set.length(); i++) {
			s1._transitionSet.add(new Transition(set.charAt(i), s2));
		}
		a._deterministic = true;
		a.reduce();
		return a;
	}
	
	/**
	 * Constructs sub-automaton corresponding to decimal numbers of 
	 * length x.substring(n).length().
	 */
	private static State anyOfRightLength(String x, int n) {
		State s = new State();
		if (x.length() == n) {
			s.setAccept(true);
		} else {
			s.addTransition(new Transition('0', '9', anyOfRightLength(x, n + 1)));
		}
		return s;
	}
	
	/**
	 * Constructs sub-automaton corresponding to decimal numbers of value 
	 * at least x.substring(n) and length x.substring(n).length().
	 */
	private static State atLeast(String x, int n, Collection<State> initials, boolean zeros) {
		State s = new State();
		if (x.length() == n) {
			s.setAccept(true);
		} else {
			if (zeros) {
				initials.add(s);
			}
			char c = x.charAt(n);
			s.addTransition(new Transition(c, atLeast(x, n + 1,
					initials, zeros && c == '0')));
			if (c < '9') {
				s.addTransition(new Transition((char) (c + 1),
						'9', anyOfRightLength(x, n + 1)));
			}
		}
		return s;
	}
	
	/**
	 * Constructs sub-automaton corresponding to decimal numbers of value 
	 * at most x.substring(n) and length x.substring(n).length().
	 */
	private static State atMost(String x, int n) {
		State s = new State();
		if (x.length() == n) {
			s.setAccept(true);
		} else {
			char c = x.charAt(n);
			s.addTransition(new Transition(c, atMost(x, (char)n + 1)));
			if (c > '0') {
				s.addTransition(new Transition('0', (char) (c - 1),
						anyOfRightLength(x, n + 1)));
			}
		}
		return s;
	}
	
	/**
	 * Constructs sub-automaton corresponding to decimal numbers of value 
	 * between x.substring(n) and y.substring(n) and of
	 * length x.substring(n).length() (which must be equal to y.substring(n).length()).
	 */
	private static State between(String x, String y, int n, Collection<State> initials, boolean zeros) {
		State s = new State();
		if (x.length() == n) {
			s.setAccept(true);
		} else {
			if (zeros) {
				initials.add(s);
			}
			char cx = x.charAt(n);
			char cy = y.charAt(n);
			if (cx == cy) {
				s.addTransition(new Transition(cx, between(x, y, n + 1, initials,
						zeros && cx == '0')));
			} else { // cx<cy
				s.addTransition(new Transition(cx, atLeast(x, n + 1, initials,
						zeros && cx == '0')));
				s.addTransition(new Transition(cy, atMost(y, n + 1)));
				if (cx + 1 < cy) {
					s.addTransition(new Transition((char) (cx + 1), (char) (cy - 1),
							anyOfRightLength(x, n + 1)));
				}
			}
		}
		return s;
	}
	
	/** 
	 * Returns a new automaton that accepts strings representing 
	 * decimal non-negative integers in the given interval.
	 * @param min minimal value of interval
	 * @param max maximal value of inverval (both end points are included in the interval)
	 * @param digits if &gt;0, use fixed number of digits (strings must be prefixed
	 *               by 0's to obtain the right length) -
	 *               otherwise, the number of digits is not fixed
	 * @exception IllegalArgumentException if min&gt;max or if numbers in the interval cannot be expressed
	 *                                     with the given fixed number of digits
	 */
	public static Automaton makeInterval(int min, int max, int digits) throws IllegalArgumentException {
		Automaton a = new Automaton();
		String x = Integer.toString(min);
		String y = Integer.toString(max);
		if (min > max || (digits > 0 && y.length() > digits)) {
			throw new IllegalArgumentException();
		}
		int d;
		if (digits > 0) {
			d = digits;
		} else {
			d = y.length();
		}
		{
		}
		StringBuilder bx = new StringBuilder();
		for (int i = x.length(); i < d; i++) {
			bx.append('0');
		}
		bx.append(x);
		x = bx.toString();
		StringBuilder by = new StringBuilder();
		for (int i = y.length(); i < d; i++) {
			by.append('0');
		}
		by.append(y);
		y = by.toString();
		Collection<State> initials = new ArrayList<State>();
		a._initial = between(x, y, 0, initials, digits <= 0);
		if (digits <= 0) {
			ArrayList<StatePair> pairs = new ArrayList<StatePair>();
			for (State p : initials) {
				if (a._initial != p) {
					pairs.add(new StatePair(a._initial, p));
				}
			}
			a.addEpsilons(pairs);
			a._initial.addTransition(new Transition('0', a._initial));
			a._deterministic = false;
		} else {
			a._deterministic = true;
		}
		a.checkMinimizeAlways();
		return a;
	}
	
	/** 
	 * Returns a new (deterministic) automaton that accepts the single given string.
	 */
	public static Automaton makeString(String s) {
		Automaton a = new Automaton();
		a._singleton = s;
		a._deterministic = true;
		return a;
	}

	/**
	 * Constructs automaton that accept strings representing nonnegative integers
	 * that are not larger than the given value.
	 * @param n string representation of maximum value
	 */
	public static Automaton makeMaxInteger(String n) {
		int i = 0;
		while (i < n.length() && n.charAt(i) == '0') {
			i++;
		}
		StringBuilder b = new StringBuilder();
		b.append("0*(0|");
		if (i < n.length()) {
			b.append("[0-9]{1,").append(n.length() - i - 1).append("}|");
		}
		maxInteger(n.substring(i), 0, b);
		b.append(")");
		return Automaton.minimize((new RegExp(b.toString())).toAutomaton());
	}

	private static void maxInteger(String n, int i, StringBuilder b) {
		b.append('(');
		if (i < n.length()) {
			char c = n.charAt(i);
			if (c != '0') {
				b.append("[0-").append((char) (c - 1)).append("][0-9]{").append(n.length() - i - 1).append("}|");
			}
			b.append(c);
			maxInteger(n, i + 1, b);
		}
		b.append(')');
	}
}
