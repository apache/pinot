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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Special automata operations.
 */
public final class SpecialOperations {
	
	private SpecialOperations() {
	}

	/**
	 * Reverses the language of the given (non-singleton) automaton while returning
	 * the set of new initial states.
	 */
	public static Set<State> reverse(Automaton a) {
		// reverse all edges
		HashMap<State, HashSet<Transition>> m = new HashMap<State, HashSet<Transition>>();
		Set<State> states = a.getStates();
		Set<State> accept = a.getAcceptStates();
		for (State r : states) {
			m.put(r, new HashSet<Transition>());
			r._accept = false;
		}
		for (State r : states) {
			for (Transition t : r.getTransitionSet()) {
				m.get(t._to).add(new Transition(t._min, t._max, r));
			}
		}
		for (State r : states) {
			r._transitionSet = m.get(r);
		}
		// make new initial+final states
		a._initial._accept = true;
		a._initial = new State();
		for (State r : accept) {
			a._initial.addEpsilon(r); // ensures that all initial states are reachable
		}
		a._deterministic = false;
		return accept;
	}

	/**
	 * Returns an automaton that accepts the overlap of strings that in more than one way can be split into
	 * a left part being accepted by <code>a1</code> and a right part being accepted by
	 * <code>a2</code>.
	 */
	public static Automaton overlap(Automaton a1, Automaton a2) {
		Automaton b1 = a1.cloneExpanded();
		b1.determinize();
		acceptToAccept(b1);
		Automaton b2 = a2.cloneExpanded();
		reverse(b2);
		b2.determinize();
		acceptToAccept(b2);
		reverse(b2);
		b2.determinize();
		return b1.intersection(b2).minus(BasicAutomata.makeEmptyString());
	}
	
	private static void acceptToAccept(Automaton a) {
		State s = new State();
		for (State r : a.getAcceptStates()) {
			s.addEpsilon(r);
		}
		a._initial = s;
		a._deterministic = false;
	}
	
	/** 
	 * Returns an automaton that accepts the single chars that occur 
	 * in strings that are accepted by the given automaton. 
	 * Never modifies the input automaton.
	 */
	public static Automaton singleChars(Automaton a) {
		Automaton b = new Automaton();
		State s = new State();
		b._initial = s;
		State q = new State();
		q._accept = true;
		if (a.isSingleton()) {
			for (int i = 0; i < a._singleton.length(); i++) {
				s._transitionSet.add(new Transition(a._singleton.charAt(i), q));
			}
		} else {
			for (State p : a.getStates()) {
				for (Transition t : p._transitionSet) {
					s._transitionSet.add(new Transition(t._min, t._max, q));
				}
			}
		}

		b._deterministic = true;
		b.removeDeadTransitions();
		return b;
	}
	
	/**
	 * Returns an automaton that accepts the trimmed language of the given
	 * automaton. The resulting automaton is constructed as follows: 1) Whenever
	 * a <code>c</code> character is allowed in the original automaton, one or
	 * more <code>set</code> characters are allowed in the new automaton. 2)
	 * The automaton is prefixed and postfixed with any number of
	 * <code>set</code> characters.
	 * @param set set of characters to be trimmed
	 * @param c canonical trim character (assumed to be in <code>set</code>)
	 */
	public static Automaton trim(Automaton a, String set, char c) {
		a = a.cloneExpandedIfRequired();
		State f = new State();
		addSetTransitions(f, set, f);
		f._accept = true;
		for (State s : a.getStates()) {
			State r = s.step(c);
			if (r != null) {
				// add inner
				State q = new State();
				addSetTransitions(q, set, q);
				addSetTransitions(s, set, q);
				q.addEpsilon(r);
			}
			// add postfix
			if (s._accept) {
				s.addEpsilon(f);
			}
		}
		// add prefix
		State p = new State();
		addSetTransitions(p, set, p);
		p.addEpsilon(a._initial);
		a._initial = p;
		a._deterministic = false;
		a.removeDeadTransitions();
		a.checkMinimizeAlways();
		return a;
	}
	
	private static void addSetTransitions(State s, String set, State p) {
		for (int n = 0; n < set.length(); n++) {
			s._transitionSet.add(new Transition(set.charAt(n), p));
		}
	}
	
	/**
	 * Returns an automaton that accepts the compressed language of the given
	 * automaton. Whenever a <code>c</code> character is allowed in the
	 * original automaton, one or more <code>set</code> characters are allowed
	 * in the new automaton.
	 * @param set set of characters to be compressed
	 * @param c canonical compress character (assumed to be in <code>set</code>)
	 */
	public static Automaton compress(Automaton a, String set, char c) {
		a = a.cloneExpandedIfRequired();
		for (State s : a.getStates()) {
			State r = s.step(c);
			if (r != null) {
				// add inner
				State q = new State();
				addSetTransitions(q, set, q);
				addSetTransitions(s, set, q);
				q.addEpsilon(r);
			}
		}
		// add prefix
		a._deterministic = false;
		a.removeDeadTransitions();
		a.checkMinimizeAlways();
		return a;
	}
	
	/**
	 * Returns an automaton where all transition labels have been substituted.
	 * <p>
	 * Each transition labeled <code>c</code> is changed to a set of
	 * transitions, one for each character in <code>map(c)</code>. If
	 * <code>map(c)</code> is null, then the transition is unchanged.
	 * @param map map from characters to sets of characters (where characters 
	 *            are <code>Character</code> objects)
	 */
	public static Automaton subst(Automaton a, Map<Character, Set<Character>> map) {
		if (map.isEmpty()) {
			return a.cloneIfRequired();
		}
		Set<Character> ckeys = new TreeSet<>(map.keySet());
		char[] keys = new char[ckeys.size()];
		int j = 0;
		for (Character c : ckeys) {
			keys[j++] = c;
		}
		a = a.cloneExpandedIfRequired();
		for (State s : a.getStates()) {
			Set<Transition> st = s._transitionSet;
			s.resetTransitions();
			for (Transition t : st) {
				int index = findIndex(t._min, keys);
				while (t._min <= t._max) {
					if (keys[index] > t._min) {
						char m = (char)(keys[index] - 1);
						if (t._max < m) {
							m = t._max;
						}
						s._transitionSet.add(new Transition(t._min, m, t._to));
						if (m + 1 > Character.MAX_VALUE) {
							break;
						}
						t._min = (char)(m + 1);
					} else if (keys[index] < t._min) {
						char m;
						if (index + 1 < keys.length) {
							m = (char) (keys[++index] - 1);
						} else {
							m = Character.MAX_VALUE;
						}

						if (t._max < m) {
							m = t._max;
						}
						s._transitionSet.add(new Transition(t._min, m, t._to));
						if (m + 1 > Character.MAX_VALUE) {
							break;
						}
						t._min = (char)(m + 1);
					} else { // found t.min in substitution map
						for (Character c : map.get(t._min)) {
							s._transitionSet.add(new Transition(c, t._to));
						}
						if (t._min + 1 > Character.MAX_VALUE) {
							break;
						}
						t._min++;
						if (index + 1 < keys.length && keys[index + 1] == t._min) {
							index++;
						}
					}
				}
			}
		}
		a._deterministic = false;
		a.removeDeadTransitions();
		a.checkMinimizeAlways();
		return a;
	}

	/** 
	 * Finds the largest entry whose value is less than or equal to c, 
	 * or 0 if there is no such entry. 
	 */
	static int findIndex(char c, char[] points) {
		int a = 0;
		int b = points.length;
		while (b - a > 1) {
			int d = (a + b) >>> 1;
			if (points[d] > c) {
				b = d;
			} else if (points[d] < c) {
				a = d;
			} else {
				return d;
			}
		}
		return a;
	}
	
	/**
	 * Returns an automaton where all transitions of the given char are replaced by a string.
	 * @param c char
	 * @param s string
	 * @return new automaton
	 */
	public static Automaton subst(Automaton a, char c, String s) {
		a = a.cloneExpandedIfRequired();
		Set<StatePair> epsilons = new HashSet<StatePair>();
		for (State p : a.getStates()) {
			Set<Transition> st = p._transitionSet;
			p.resetTransitions();
			for (Transition t : st) {
				if (t._max < c || t._min > c) {
					p._transitionSet.add(t);
				} else {
					if (t._min < c) {
						p._transitionSet.add(new Transition(t._min, (char) (c - 1), t._to));
					}
					if (t._max > c) {
						p._transitionSet.add(new Transition((char) (c + 1), t._max, t._to));
					}
					if (s.length() == 0) {
						epsilons.add(new StatePair(p, t._to));
					} else {
						State q = p;
						for (int i = 0; i < s.length(); i++) {
							State r;
							if (i + 1 == s.length()) {
								r = t._to;
							} else {
								r = new State();
							}
							q._transitionSet.add(new Transition(s.charAt(i), r));
							q = r;
						}
					}
				}
			}
		}
		a.addEpsilons(epsilons);
		a._deterministic = false;
		a.removeDeadTransitions();
		a.checkMinimizeAlways();
		return a;
	}
	
	/**
	 * Returns true if the language of this automaton is finite.
	 */
	public static boolean isFinite(Automaton a) {
		if (a.isSingleton()) {
			return true;
		}
		return isFinite(a._initial, new HashSet<>(), new HashSet<>());
	}
	
	/** 
	 * Checks whether there is a loop containing s. (This is sufficient since 
	 * there are never transitions to dead states.) 
	 */
	private static boolean isFinite(State s, HashSet<State> path, HashSet<State> visited) {
		path.add(s);
		for (Transition t : s._transitionSet) {
			if (path.contains(t._to) || (!visited.contains(t._to) && !isFinite(t._to, path, visited))) {
				return false;
			}
		}

		path.remove(s);
		visited.add(s);
		return true;
	}
	
	/**
	 * Returns the longest string that is a prefix of all accepted strings and
	 * visits each state at most once.
	 * @return common prefix
	 */
	public static String getCommonPrefix(Automaton a) {
		if (a.isSingleton()) {
			return a._singleton;
		}
		StringBuilder b = new StringBuilder();
		HashSet<State> visited = new HashSet<State>();
		State s = a._initial;
		boolean done;
		do {
			done = true;
			visited.add(s);
			if (!s._accept && s._transitionSet.size() == 1) {
				Transition t = s._transitionSet.iterator().next();
				if (t._min == t._max && !visited.contains(t._to)) {
					b.append(t._min);
					s = t._to;
					done = false;
				}
			}
		} while (!done);
		return b.toString();
	}
}
