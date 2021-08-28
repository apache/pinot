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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Automata operations involving shuffling.
 */
final public class ShuffleOperations {
	
	private ShuffleOperations() {}

	/** 
	 * Returns an automaton that accepts the shuffle (interleaving) of 
	 * the languages of the given automata.
	 * As a side-effect, both automata are determinized, if not already deterministic.     
	 * Never modifies the input automata languages.
	 * <p>
	 * Complexity: quadratic in number of states (if already deterministic). 
	 * <p>
	 * <dl><dt><b>Author:</b></dt><dd>Torben Ruby 
	 * &lt;<a href="mailto:ruby@daimi.au.dk">ruby@daimi.au.dk</a>&gt;</dd></dl>
	 */
	public static Automaton shuffle(Automaton a1, Automaton a2) {
		a1.determinize();
		a2.determinize();
		Transition[][] transitions1 = Automaton.getSortedTransitions(a1.getStates());
		Transition[][] transitions2 = Automaton.getSortedTransitions(a2.getStates());
		Automaton c = new Automaton();
		LinkedList<StatePair> worklist = new LinkedList<StatePair>();
		HashMap<StatePair, StatePair> newstates = new HashMap<StatePair, StatePair>();
		State s = new State();
		c.initial = s;
		StatePair p = new StatePair(s, a1.initial, a2.initial);
		worklist.add(p);
		newstates.put(p, p);
		while (worklist.size() > 0) {
			p = worklist.removeFirst();
			p.s.accept = p.s1.accept && p.s2.accept;
			Transition[] t1 = transitions1[p.s1.number];
			for (int n1 = 0; n1 < t1.length; n1++) {
				StatePair q = new StatePair(t1[n1].to, p.s2);
				StatePair r = newstates.get(q);
				if (r == null) {
					q.s = new State();
					worklist.add(q);
					newstates.put(q, q);
					r = q;
				}
				p.s.transitions.add(new Transition(t1[n1].min, t1[n1].max, r.s));
			}
			Transition[] t2 = transitions2[p.s2.number];
			for (int n2 = 0; n2 < t2.length; n2++) {
				StatePair q = new StatePair(p.s1, t2[n2].to);
				StatePair r = newstates.get(q);
				if (r == null) {
					q.s = new State();
					worklist.add(q);
					newstates.put(q, q);
					r = q;
				}
				p.s.transitions.add(new Transition(t2[n2].min, t2[n2].max, r.s));
			}
		}
		c.deterministic = false;
		c.removeDeadTransitions();
		c.checkMinimizeAlways();
		return c;
	}
	
	/**
	 * Returns a string that is an interleaving of strings that are accepted by
	 * <code>ca</code> but not by <code>a</code>. If no such string
	 * exists, null is returned. As a side-effect, <code>a</code> is determinized, 
	 * if not already deterministic. Only interleavings that respect
	 * the suspend/resume markers (two BMP private code points) are considered if the markers are non-null. 
	 * Also, interleavings never split surrogate pairs.
	 * <p>
	 * Complexity: proportional to the product of the numbers of states (if <code>a</code>
	 * is already deterministic).
	 */ 
	public static String shuffleSubsetOf(Collection<Automaton> ca, Automaton a, Character suspend_shuffle, Character resume_shuffle) {
		if (ca.size() == 0)
			return null;
		if (ca.size() == 1) {
			Automaton a1 = ca.iterator().next();
			if (a1.isSingleton()) {
				if (a.run(a1.singleton))
					return null;
				else
					return a1.singleton;
			}
			if (a1 == a)
				return null;
		}
		a.determinize();
		Transition[][][] ca_transitions = new Transition[ca.size()][][];
		int i = 0;
		for (Automaton a1 : ca)
			ca_transitions[i++] = Automaton.getSortedTransitions(a1.getStates());
		Transition[][] a_transitions = Automaton.getSortedTransitions(a.getStates());
		TransitionComparator tc = new TransitionComparator(false);
		ShuffleConfiguration init = new ShuffleConfiguration(ca, a);
		LinkedList<ShuffleConfiguration> pending = new LinkedList<ShuffleConfiguration>();
		Set<ShuffleConfiguration> visited = new HashSet<ShuffleConfiguration>();
		pending.add(init);
		visited.add(init);
		while (!pending.isEmpty()) {
			ShuffleConfiguration c = pending.removeFirst();
			boolean good = true;
			for (int i1 = 0; i1 < ca.size(); i1++)
				if (!c.ca_states[i1].accept) {
					good = false;
					break;
				}
			if (c.a_state.accept)
				good = false;
			if (good) {
				StringBuilder sb = new StringBuilder();
				while (c.prev != null) {
					sb.append(c.min);
					c = c.prev;
				}
				StringBuilder sb2 = new StringBuilder();
				for (int j = sb.length() - 1; j >= 0; j--)
					sb2.append(sb.charAt(j));
				return sb2.toString();
			}
			Transition[] ta2 = a_transitions[c.a_state.number];
			for (int i1 = 0; i1 < ca.size(); i1++) {
				if (c.shuffle_suspended)
					i1 = c.suspended1;
				loop: for (Transition t1 : ca_transitions[i1][c.ca_states[i1].number]) {
					List<Transition> lt = new ArrayList<Transition>();
					int j = Arrays.binarySearch(ta2, t1, tc);
					if (j < 0)
						j = -j - 1;
					if (j > 0 && ta2[j - 1].max >= t1.min)
						j--;
					while (j < ta2.length) {
						Transition t2 = ta2[j++];
						char min = t1.min;
						char max = t1.max;
						if (t2.min > min)
							min = t2.min;
						if (t2.max < max)
							max = t2.max;
						if (min <= max) {
							add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, min, max);
							lt.add(new Transition(min, max, null));
						} else
							break;
					}
					Transition[] at = lt.toArray(new Transition[lt.size()]);
					Arrays.sort(at, tc);
					char min = t1.min;
					for (int k = 0; k < at.length; k++) {
						if (at[k].min > min)
							break;
						if (at[k].max >= t1.max)
							continue loop;
						min = (char)(at[k].max + 1);
					}
					ShuffleConfiguration nc = new ShuffleConfiguration(c, i1, t1.to, min);
					StringBuilder sb = new StringBuilder();
					ShuffleConfiguration b = nc;
					while (b.prev != null) {
						sb.append(b.min);
						b = b.prev;
					}
					StringBuilder sb2 = new StringBuilder();
					for (int m = sb.length() - 1; m >= 0; m--)
						sb2.append(sb.charAt(m));
					if (c.shuffle_suspended)
						sb2.append(BasicOperations.getShortestExample(nc.ca_states[c.suspended1], true));
					for (i1 = 0; i1 < ca.size(); i1++)
						if (!c.shuffle_suspended || i1 != c.suspended1)
							sb2.append(BasicOperations.getShortestExample(nc.ca_states[i1], true));
					return sb2.toString();
				}
				if (c.shuffle_suspended)
					break;
			}
		}
		return null;
	}

	private static void add(Character suspend_shuffle, Character resume_shuffle, 
			                LinkedList<ShuffleConfiguration> pending, Set<ShuffleConfiguration> visited, 
			                ShuffleConfiguration c, int i1, Transition t1, Transition t2, char min, char max) {
		final char HIGH_SURROGATE_BEGIN = '\uD800'; 
		final char HIGH_SURROGATE_END = '\uDBFF'; 
		if (suspend_shuffle != null && min <= suspend_shuffle && suspend_shuffle <= max && min != max) {
			if (min < suspend_shuffle)
				add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, min, (char)(suspend_shuffle - 1));
			add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, suspend_shuffle, suspend_shuffle);
			if (suspend_shuffle < max)
				add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, (char)(suspend_shuffle + 1), max);
		} else if (resume_shuffle != null && min <= resume_shuffle && resume_shuffle <= max && min != max) {
			if (min < resume_shuffle)
				add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, min, (char)(resume_shuffle - 1));
			add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, resume_shuffle, resume_shuffle);
			if (resume_shuffle < max)
				add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, (char)(resume_shuffle + 1), max);
		} else if (min < HIGH_SURROGATE_BEGIN && max >= HIGH_SURROGATE_BEGIN) {
			add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, min, (char)(HIGH_SURROGATE_BEGIN - 1));
			add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, HIGH_SURROGATE_BEGIN, max);
		} else if (min <= HIGH_SURROGATE_END && max > HIGH_SURROGATE_END) {
			add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, min, HIGH_SURROGATE_END);
			add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, (char)(HIGH_SURROGATE_END + 1), max);
		} else {
			ShuffleConfiguration nc = new ShuffleConfiguration(c, i1, t1.to, t2.to, min);
			if (suspend_shuffle != null && min == suspend_shuffle) {
				nc.shuffle_suspended = true;
				nc.suspended1 = i1;
			} else if (resume_shuffle != null && min == resume_shuffle)
				nc.shuffle_suspended = false;
			if (min >= HIGH_SURROGATE_BEGIN && min <= HIGH_SURROGATE_BEGIN) {
				nc.shuffle_suspended = true;
				nc.suspended1 = i1;
				nc.surrogate = true;
			}
			if (!visited.contains(nc)) {
				pending.add(nc);
				visited.add(nc);
			}
		}
	}

	static class ShuffleConfiguration {
		
		ShuffleConfiguration prev;
		State[] ca_states;
		State a_state;
		char min;
		int hash;
		boolean shuffle_suspended;
		boolean surrogate;
		int suspended1;
		
		@SuppressWarnings("unused")
		private ShuffleConfiguration() {}
		
		ShuffleConfiguration(Collection<Automaton> ca, Automaton a) {
			ca_states = new State[ca.size()];
			int i = 0;
			for (Automaton a1 : ca)
				ca_states[i++] = a1.getInitialState();
			a_state = a.getInitialState();
			computeHash();
		}
		
		ShuffleConfiguration(ShuffleConfiguration c, int i1, State s1, char min) {
			prev = c;
			ca_states = c.ca_states.clone();
			a_state = c.a_state;
			ca_states[i1] = s1;
			this.min = min;
			computeHash();
		}

		ShuffleConfiguration(ShuffleConfiguration c, int i1, State s1, State s2, char min) {
			prev = c;
			ca_states = c.ca_states.clone();
			a_state = c.a_state;
			ca_states[i1] = s1;
			a_state = s2;
			this.min = min;
			if (!surrogate) {
				shuffle_suspended = c.shuffle_suspended;
				suspended1 = c.suspended1;
			}
			computeHash();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ShuffleConfiguration) {
				ShuffleConfiguration c = (ShuffleConfiguration)obj;
				return shuffle_suspended == c.shuffle_suspended &&
					   surrogate == c.surrogate &&
					   suspended1 == c.suspended1 &&
					   Arrays.equals(ca_states, c.ca_states) &&
					   a_state == c.a_state;
			}
			return false;
		}

		@Override
		public int hashCode() {
			return hash;
		}
		
		private void computeHash() {
			hash = 0;
			for (int i = 0; i < ca_states.length; i++)
				hash ^= ca_states[i].hashCode();
			hash ^= a_state.hashCode() * 100;
			if (shuffle_suspended || surrogate)
				hash += suspended1;
		}
	}
}
