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
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Operations for minimizing automata.
 */
public final class MinimizationOperations {

	private MinimizationOperations() {}

	/**
	 * Minimizes (and determinizes if not already deterministic) the given automaton.
	 * @see Automaton#setMinimization(int)
	 */
	public static void minimize(Automaton a) {
		if (!a.isSingleton()) {
			switch (Automaton.minimization) {
			case Automaton.MINIMIZE_HUFFMAN:
				minimizeHuffman(a);
				break;
			case Automaton.MINIMIZE_BRZOZOWSKI:
				minimizeBrzozowski(a);
				break;
			case Automaton.MINIMIZE_VALMARI:
				minimizeValmari(a);
				break;
			default:
				minimizeHopcroft(a);
			}
		}
		a.recomputeHashCode();
	}
	
	private static boolean statesAgree(Transition[][] transitions, boolean[][] mark, int n1, int n2) {
		Transition[] t1 = transitions[n1];
		Transition[] t2 = transitions[n2];
		for (int k1 = 0, k2 = 0; k1 < t1.length && k2 < t2.length;) {
			if (t1[k1]._max < t2[k2]._min) {
        k1++;
      } else if (t2[k2]._max < t1[k1]._min) {
        k2++;
      } else {
				int m1 = t1[k1]._to._number;
				int m2 = t2[k2]._to._number;
				if (m1 > m2) {
					int t = m1;
					m1 = m2;
					m2 = t;
				}
				if (mark[m1][m2]) {
          return false;
        }
				if (t1[k1]._max < t2[k2]._max) {
          k1++;
        } else {
          k2++;
        }
			}
		}
		return true;
	}

	private static void addTriggers(Transition[][] transitions,
			ArrayList<ArrayList<HashSet<IntPair>>> triggers, int n1, int n2) {
		Transition[] t1 = transitions[n1];
		Transition[] t2 = transitions[n2];
		for (int k1 = 0, k2 = 0; k1 < t1.length && k2 < t2.length;) {
			if (t1[k1]._max < t2[k2]._min) {
        k1++;
      } else if (t2[k2]._max < t1[k1]._min) {
        k2++;
      } else {
				if (t1[k1]._to != t2[k2]._to) {
					int m1 = t1[k1]._to._number;
					int m2 = t2[k2]._to._number;
					if (m1 > m2) {
						int t = m1;
						m1 = m2;
						m2 = t;
					}
					if (triggers.get(m1).get(m2) == null) {
            triggers.get(m1).set(m2, new HashSet<IntPair>());
          }
					triggers.get(m1).get(m2).add(new IntPair(n1, n2));
				}
				if (t1[k1]._max < t2[k2]._max) {
          k1++;
        } else {
          k2++;
        }
			}
		}
	}

	private static void markPair(boolean[][] mark, ArrayList<ArrayList<HashSet<IntPair>>> triggers, int n1, int n2) {
		mark[n1][n2] = true;
		if (triggers.get(n1).get(n2) != null) {
			for (IntPair p : triggers.get(n1).get(n2)) {
				int m1 = p.n1;
				int m2 = p.n2;
				if (m1 > m2) {
					int t = m1;
					m1 = m2;
					m2 = t;
				}
				if (!mark[m1][m2]) {
          markPair(mark, triggers, m1, m2);
        }
			}
		}
	}

	private static <T> void initialize(ArrayList<T> list, int size) {
		for (int i = 0; i < size; i++) {
      list.add(null);
    }
	}
	
	/** 
	 * Minimizes the given automaton using Huffman's algorithm. 
	 */
	public static void minimizeHuffman(Automaton a) {
		a.determinize();
		a.totalize();
		Set<State> ss = a.getStates();
		Transition[][] transitions = new Transition[ss.size()][];
		State[] states = ss.toArray(new State[ss.size()]);
		boolean[][] mark = new boolean[states.length][states.length];
		ArrayList<ArrayList<HashSet<IntPair>>> triggers = new ArrayList<ArrayList<HashSet<IntPair>>>();
		for (int n1 = 0; n1 < states.length; n1++) {
			ArrayList<HashSet<IntPair>> v = new ArrayList<HashSet<IntPair>>();
			initialize(v, states.length);
			triggers.add(v);
		}
		// initialize marks based on acceptance status and find transition arrays
		for (int n1 = 0; n1 < states.length; n1++) {
			states[n1]._number = n1;
			transitions[n1] = states[n1].getSortedTransitionArray(false);
			for (int n2 = n1 + 1; n2 < states.length; n2++) {
        if (states[n1]._accept != states[n2]._accept) {
          mark[n1][n2] = true;
        }
      }
		}
		// for all pairs, see if states agree
		for (int n1 = 0; n1 < states.length; n1++) {
      for (int n2 = n1 + 1; n2 < states.length; n2++) {
        if (!mark[n1][n2]) {
          if (statesAgree(transitions, mark, n1, n2)) {
            addTriggers(transitions, triggers, n1, n2);
          } else {
            markPair(mark, triggers, n1, n2);
          }
        }
      }
    }
		// assign equivalence class numbers to states
		int numclasses = 0;
		for (int n = 0; n < states.length; n++) {
      states[n]._number = -1;
    }
		for (int n1 = 0; n1 < states.length; n1++) {
      if (states[n1]._number == -1) {
        states[n1]._number = numclasses;
        for (int n2 = n1 + 1; n2 < states.length; n2++) {
          if (!mark[n1][n2]) {
            states[n2]._number = numclasses;
          }
        }
        numclasses++;
      }
    }
		// make a new state for each equivalence class
		State[] newstates = new State[numclasses];
		for (int n = 0; n < numclasses; n++) {
      newstates[n] = new State();
    }
		// select a class representative for each class and find the new initial
		// state
		for (int n = 0; n < states.length; n++) {
			newstates[states[n]._number]._number = n;
			if (states[n] == a._initial) {
        a._initial = newstates[states[n]._number];
      }
		}
		// build transitions and set acceptance
		for (int n = 0; n < numclasses; n++) {
			State s = newstates[n];
			s._accept = states[s._number]._accept;
			for (Transition t : states[s._number]._transitionSet) {
        s._transitionSet.add(new Transition(t._min, t._max, newstates[t._to._number]));
      }
		}
		a.removeDeadTransitions();
	}
	
	/** 
	 * Minimizes the given automaton using Brzozowski's algorithm. 
	 */
	public static void minimizeBrzozowski(Automaton a) {
		if (a.isSingleton()) {
      return;
    }
		BasicOperations.determinize(a, SpecialOperations.reverse(a));
		BasicOperations.determinize(a, SpecialOperations.reverse(a));
	}
	
	/** 
	 * Minimizes the given automaton using Hopcroft's algorithm. 
	 */
	public static void minimizeHopcroft(Automaton a) {
		a.determinize();
		Set<Transition> tr = a._initial.getTransitionSet();
		if (tr.size() == 1) {
			Transition t = tr.iterator().next();
			if (t._to == a._initial && t._min == Character.MIN_VALUE && t._max == Character.MAX_VALUE) {
        return;
      }
		}
		a.totalize();
		// make arrays for numbered states and effective alphabet
		Set<State> ss = a.getStates();
		State[] states = new State[ss.size()];
		int number = 0;
		for (State q : ss) {
			states[number] = q;
			q._number = number++;
		}
		char[] sigma = a.getStartPoints();
		// initialize data structures
		ArrayList<ArrayList<LinkedList<State>>> reverse = new ArrayList<ArrayList<LinkedList<State>>>();
		for (int q = 0; q < states.length; q++) {
			ArrayList<LinkedList<State>> v = new ArrayList<LinkedList<State>>();
			initialize(v, sigma.length);
			reverse.add(v);
		}
		boolean[][] reverse_nonempty = new boolean[states.length][sigma.length];
		ArrayList<LinkedList<State>> partition = new ArrayList<LinkedList<State>>();
		initialize(partition, states.length);
		int[] block = new int[states.length];
		StateList[][] active = new StateList[states.length][sigma.length];
		StateListNode[][] active2 = new StateListNode[states.length][sigma.length];
		LinkedList<IntPair> pending = new LinkedList<IntPair>();
		boolean[][] pending2 = new boolean[sigma.length][states.length];
		ArrayList<State> split = new ArrayList<State>();
		boolean[] split2 = new boolean[states.length];
		ArrayList<Integer> refine = new ArrayList<Integer>();
		boolean[] refine2 = new boolean[states.length];
		ArrayList<ArrayList<State>> splitblock = new ArrayList<ArrayList<State>>();
		initialize(splitblock, states.length);
		for (int q = 0; q < states.length; q++) {
			splitblock.set(q, new ArrayList<State>());
			partition.set(q, new LinkedList<State>());
			for (int x = 0; x < sigma.length; x++) {
				reverse.get(q).set(x, new LinkedList<State>());
				active[q][x] = new StateList();
			}
		}
		// find initial partition and reverse edges
		for (int q = 0; q < states.length; q++) {
			State qq = states[q];
			int j;
			if (qq._accept) {
        j = 0;
      } else {
        j = 1;
      }
			partition.get(j).add(qq);
			block[qq._number] = j;
			for (int x = 0; x < sigma.length; x++) {
				char y = sigma[x];
				State p = qq.step(y);
				reverse.get(p._number).get(x).add(qq);
				reverse_nonempty[p._number][x] = true;
			}
		}
		// initialize active sets
		for (int j = 0; j <= 1; j++) {
      for (int x = 0; x < sigma.length; x++) {
        for (State qq : partition.get(j)) {
          if (reverse_nonempty[qq._number][x]) {
            active2[qq._number][x] = active[j][x].add(qq);
          }
        }
      }
    }
		// initialize pending
		for (int x = 0; x < sigma.length; x++) {
			int a0 = active[0][x]._size;
			int a1 = active[1][x]._size;
			int j;
			if (a0 <= a1) {
        j = 0;
      } else {
        j = 1;
      }
			pending.add(new IntPair(j, x));
			pending2[x][j] = true;
		}
		// process pending until fixed point
		int k = 2;
		while (!pending.isEmpty()) {
			IntPair ip = pending.removeFirst();
			int p = ip.n1;
			int x = ip.n2;
			pending2[x][p] = false;
			// find states that need to be split off their blocks
			for (StateListNode m = active[p][x]._first; m != null; m = m._next) {
        for (State s : reverse.get(m._q._number).get(x)) {
          if (!split2[s._number]) {
            split2[s._number] = true;
            split.add(s);
            int j = block[s._number];
            splitblock.get(j).add(s);
            if (!refine2[j]) {
              refine2[j] = true;
              refine.add(j);
            }
          }
        }
      }
			// refine blocks
			for (int j : refine) {
				if (splitblock.get(j).size() < partition.get(j).size()) {
					LinkedList<State> b1 = partition.get(j);
					LinkedList<State> b2 = partition.get(k);
					for (State s : splitblock.get(j)) {
						b1.remove(s);
						b2.add(s);
						block[s._number] = k;
						for (int c = 0; c < sigma.length; c++) {
							StateListNode sn = active2[s._number][c];
							if (sn != null && sn._stateList == active[j][c]) {
								sn.remove();
								active2[s._number][c] = active[k][c].add(s);
							}
						}
					}
					// update pending
					for (int c = 0; c < sigma.length; c++) {
						int aj = active[j][c]._size;
						int ak = active[k][c]._size;
						if (!pending2[c][j] && 0 < aj && aj <= ak) {
							pending2[c][j] = true;
							pending.add(new IntPair(j, c));
						} else {
							pending2[c][k] = true;
							pending.add(new IntPair(k, c));
						}
					}
					k++;
				}
				for (State s : splitblock.get(j)) {
          split2[s._number] = false;
        }
				refine2[j] = false;
				splitblock.get(j).clear();
			}
			split.clear();
			refine.clear();
		}
		// make a new state for each equivalence class, set initial state
		State[] newstates = new State[k];
		for (int n = 0; n < newstates.length; n++) {
			State s = new State();
			newstates[n] = s;
			for (State q : partition.get(n)) {
				if (q == a._initial) {
          a._initial = s;
        }
				s._accept = q._accept;
				s._number = q._number; // select representative
				q._number = n;
			}
		}
		// build transitions and set acceptance
		for (int n = 0; n < newstates.length; n++) {
			State s = newstates[n];
			s._accept = states[s._number]._accept;
			for (Transition t : states[s._number]._transitionSet) {
        s._transitionSet.add(new Transition(t._min, t._max, newstates[t._to._number]));
      }
		}
		a.removeDeadTransitions();
	}

	/**
	 * Minimizes the given automaton using Valmari's algorithm.
	 */
	public static void minimizeValmari(Automaton automaton) {
		automaton.determinize();
		Set<State> states = automaton.getStates();
		splitTransitions(states);
		int stateCount = states.size();
		int transitionCount = automaton.getNumberOfTransitions();
		Set<State> acceptStates = automaton.getAcceptStates();
		Partition blocks = new Partition(stateCount);
		Partition cords = new Partition(transitionCount);
		IntPair[] labels = new IntPair[transitionCount];
		int[] tails = new int[transitionCount];
		int[] heads = new int[transitionCount];
		// split transitions in 'heads', 'labels', and 'tails'
		Automaton.setStateNumbers(states);
		int number = 0;
		for (State s : automaton.getStates()) {
			for (Transition t : s.getTransitionSet()) {
				tails[number] = s._number;
				labels[number] = new IntPair(t._min, t._max);
				heads[number] = t.getDest()._number;
				number++;
			}
		}
		// make initial block partition
		for (State s : acceptStates) {
      blocks.mark(s._number);
    }
		blocks.split();
		// make initial transition partition
		if (transitionCount > 0) {
			Arrays.sort(cords._elements, new LabelComparator(labels));
			cords._setCount = cords._markedElementCount[0] = 0;
			IntPair a = labels[cords._elements[0]];
			for (int i = 0; i < transitionCount; ++i) {
				int t = cords._elements[i];
				if (labels[t].n1 != a.n1 || labels[t].n2 != a.n2) {
					a = labels[t];
					cords._past[cords._setCount++] = i;
					cords._first[cords._setCount] = i;
					cords._markedElementCount[cords._setCount] = 0;
				}
				cords._setNo[t] = cords._setCount;
				cords._locations[t] = i;
			}
			cords._past[cords._setCount++] = transitionCount;
		}
		// split blocks and cords
		int[] A = new int[transitionCount];
		int[] F = new int[stateCount+1];
		makeAdjacent(A, F, heads, stateCount, transitionCount);
		for (int c = 0; c < cords._setCount; ++c) {
			for (int i = cords._first[c]; i < cords._past[c]; ++i) {
        blocks.mark(tails[cords._elements[i]]);
      }
			blocks.split();
			for (int b = 1; b < blocks._setCount; ++b) {
				for (int i = blocks._first[b]; i < blocks._past[b]; ++i) {
					for (int j = F[blocks._elements[i]]; j < F[blocks._elements[i] + 1]; ++j) {
						cords.mark(A[j]);
					}
				}
				cords.split();
			}
		}
		// build states and acceptance states
		State[] newStates = new State[blocks._setCount];
		for (int bl = 0; bl < blocks._setCount; ++bl) {
			newStates[bl] = new State();
			if (blocks._first[bl] < acceptStates.size()) {
        newStates[bl]._accept = true;
      }
		}
		// build transitions
		for (int t = 0; t < transitionCount; ++t) {
			if (blocks._locations[tails[t]] == blocks._first[blocks._setNo[tails[t]]]) {
				State tail = newStates[blocks._setNo[tails[t]]];
				State head = newStates[blocks._setNo[heads[t]]];
				tail.addTransition(new Transition((char)labels[t].n1, (char)labels[t].n2, head));
			}
		}
		automaton.setInitialState(newStates[blocks._setNo[automaton.getInitialState()._number]]);
		automaton.reduce();
	}

	private static void makeAdjacent(int[] A, int[] F, int[] K, int nn, int mm) {
		for (int q=0; q <= nn; ++q) {
      F[q] = 0;
    }
		for (int t=0; t < mm; ++t) {
      ++F[K[t]];
    }
		for (int q=0; q < nn; ++q) {
      F[q+1] += F[q];
    }
		for (int t = mm; t-- > 0;) {
      A[--F[K[t]]] = t;
    }
	}

	private static void splitTransitions(Set<State> states) {
		TreeSet<Character> pointSet = new TreeSet<Character>();
		for (State s : states) {
			for (Transition t : s.getTransitionSet()) {
				pointSet.add(t._min);
				pointSet.add(t._max);
			}
		}
		for (State s : states) {
			Set<Transition> transitions = s.getTransitionSet();
			s.resetTransitions();
			for (Transition t : transitions) {
				if (t._min == t._max) {
					s.addTransition(t);
					continue;
				}
				SortedSet<Character> headSet = pointSet.headSet(t._max, true);
				SortedSet<Character> tailSet = pointSet.tailSet(t._min, false);
				SortedSet<Character> intersection = new TreeSet<Character>(headSet);
				intersection.retainAll(tailSet);
				char start = t._min;
				for (Character c : intersection) {
					s.addTransition(new Transition(start, t._to));
					s.addTransition(new Transition(c, t._to));
					if (c - start > 1) {
            s.addTransition(new Transition((char) (start + 1), (char) (c - 1), t._to));
          }
					start = c;
				}
			}
		}
	}
	
	static class IntPair {

		int n1, n2;

		IntPair(int n1, int n2) {
			this.n1 = n1;
			this.n2 = n2;
		}
	}

	static class StateList {
		
		int _size;

		StateListNode _first;
		StateListNode _last;

		StateListNode add(State q) {
			return new StateListNode(q, this);
		}
	}

	static class StateListNode {
		
		State _q;

		StateListNode _next, _prev;

		StateList _stateList;

		StateListNode(State q, StateList _stateList) {
			this._q = q;
			this._stateList = _stateList;
			if (_stateList._size++ == 0) {
        _stateList._first = _stateList._last = this;
      } else {
				_stateList._last._next = this;
				_prev = _stateList._last;
				_stateList._last = this;
			}
		}

		void remove() {
			_stateList._size--;
			if (_stateList._first == this) {
        _stateList._first = _next;
      } else {
        _prev._next = _next;
      }
			if (_stateList._last == this) {
        _stateList._last = _prev;
      } else {
        _next._prev = _prev;
      }
		}
	}

	static class Partition {

		int[] _markedElementCount; // number of marked elements in set
		int[] _touchedSets; // sets with marked elements
		int _touchedSetCount; // number of sets with marked elements

		int _setCount;   // number of sets
		Integer[] _elements; // elements, i.e s = { elements[first[s]], elements[first[s] + 1], ..., elements[past[s]-1] }
		int[] _locations; // location of element i in elements
		int[] _setNo; // the set number element i belongs to
		int[] _first; // "first": start index of set
		int[] _past; // "past": end index of set

		Partition (int size) {
			_setCount = (size == 0) ? 0 : 1;
			_elements = new Integer[size];
			_locations = new int[size];
			_setNo = new int[size];
			_first = new int[size];
			_past = new int[size];
			_markedElementCount = new int[size];
			_touchedSets = new int[size];
			for (int i = 0; i < size; ++i) {
				_elements[i] = _locations[i] = i;
				_setNo[i] = 0;
			}
			if (_setCount != 0) {
				_first[0] = 0;
				_past[0] = size;
			}
		}

		void mark(int e) {
			int s = _setNo[e];
			int i = _locations[e];
			int j = _first[s] + _markedElementCount[s];
			_elements[i] = _elements[j];
			_locations[_elements[i]] = i;
			_elements[j] = e;
			_locations[e] = j;
			if (_markedElementCount[s]++ == 0) {
        _touchedSets[_touchedSetCount++] = s;
      }
		}

		void split() {
			while (_touchedSetCount > 0) {
				int s = _touchedSets[--_touchedSetCount];
				int j = _first[s] + _markedElementCount[s];
				if (j == _past[s]) {
					_markedElementCount[s] = 0;
					continue;
				}
				// choose the smaller of the marked and unmarked part, and make it a new set
				if (_markedElementCount[s] <= _past[s]-j) {
					_first[_setCount] = _first[s];
					_past[_setCount] = _first[s] = j;
				} else {
					_past[_setCount] = _past[s];
					_first[_setCount] = _past[s] = j;
				}
				for (int i = _first[_setCount]; i < _past[_setCount]; ++i) {
          _setNo[_elements[i]] = _setCount;
        }
				_markedElementCount[s] = _markedElementCount[_setCount++] = 0;
			}
		}
	}

	static class LabelComparator implements Comparator<Integer> {

		private IntPair[] _labels;

		LabelComparator(IntPair[] labels) {
			this._labels = labels;
		}

		public int compare(Integer i, Integer j) {
			IntPair p1 = _labels[i];
			IntPair p2 = _labels[j];
			if (p1.n1 < p2.n1) {
        return -1;
      }
			if (p1.n1 > p2.n1) {
        return 1;
      }
			if (p1.n2 < p2.n2) {
        return -1;
      }
			if (p1.n2 > p2.n2) {
        return 1;
      }
			return 0;
		}
	}
}
