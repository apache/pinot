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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Finite-state automaton with regular expression operations.
 * <p>
 * Class invariants:
 * <ul>
 * <li> An automaton is either represented explicitly (with {@link State} and {@link Transition} objects)
 *      or with a singleton string (see {@link #getSingleton()} and {@link #expandSingleton()}) in case the automaton is known to accept exactly one string.
 *      (Implicitly, all states and transitions of an automaton are reachable from its initial state.)
 * <li> Automata are always reduced (see {@link #reduce()}) 
 *      and have no transitions to dead states (see {@link #removeDeadTransitions()}).
 * <li> If an automaton is nondeterministic, then {@link #isDeterministic()} returns false (but
 *      the converse is not required).
 * <li> Automata provided as input to operations are generally assumed to be disjoint.
 * </ul>
 * <p>
 * If the states or transitions are manipulated manually, the {@link #restoreInvariant()}
 * and {@link #setDeterministic(boolean)} methods should be used afterwards to restore 
 * representation invariants that are assumed by the built-in automata operations.
 */
public class Automaton implements Serializable, Cloneable {

	public static long serialVersionUID = 10001;
	
	/**
	 * Minimize using Huffman's O(n<sup>2</sup>) algorithm. 
	 * This is the standard text-book algorithm.
	 * @see #setMinimization(int)
	 */
	public static final int MINIMIZE_HUFFMAN = 0;
	
	/**
	 * Minimize using Brzozowski's O(2<sup>n</sup>) algorithm. 
	 * This algorithm uses the reverse-determinize-reverse-determinize trick, which has a bad
	 * worst-case behavior but often works very well in practice 
	 * (even better than Hopcroft's!).
	 * @see #setMinimization(int)
	 */
	public static final int MINIMIZE_BRZOZOWSKI = 1;
	
	/**
	 * Minimize using Hopcroft's O(n log n) algorithm.
	 * This is regarded as one of the most generally efficient algorithms that exist.
	 * @see #setMinimization(int)
	 */
	public static final int MINIMIZE_HOPCROFT = 2;

	/**
	 * Minimize using Valmari's O(n + m log m) algorithm.
	 * @see #setMinimization(int)
	 */
	public static final int MINIMIZE_VALMARI = 3;

	/** Minimize always flag. */
	public static boolean minimize_always = false;

	/** Selects whether operations may modify the input automata (default: <code>false</code>). */
	public static boolean allow_mutation = false;
	
	/** Selects minimization algorithm (default: <code>MINIMIZE_HOPCROFT</code>). */
	public static int minimization = MINIMIZE_HOPCROFT;
	
	/** Initial state of this automaton. */
	State _initial;
	
	/** If true, then this automaton is definitely deterministic 
	 (i.e., there are no choices for any run, but a run may crash). */
	boolean _deterministic;
	
	/** Extra data associated with this automaton. */
	transient Object _info;
	
	/** Hash code. Recomputed by {@link #minimize()}. */
	int _hashCode;
	
	/** Singleton string. Null if not applicable. */
	String _singleton;
	
	/** 
	 * Constructs a new automaton that accepts the empty language.
	 * Using this constructor, automata can be constructed manually from
	 * {@link State} and {@link Transition} objects.
	 * @see #setInitialState(State)
	 * @see State
	 * @see Transition
	 */
	public Automaton() {
		_initial = new State();
		_deterministic = true;
		_singleton = null;
	}
	
	/** 
	 * Selects minimization algorithm (default: <code>MINIMIZE_HOPCROFT</code>). 
	 * @param algorithm minimization algorithm
	 */
	static public void setMinimization(int algorithm) {
		minimization = algorithm;
	}
	
	/**
	 * Sets or resets minimize always flag.
	 * If this flag is set, then {@link #minimize()} will automatically
	 * be invoked after all operations that otherwise may produce non-minimal automata.
	 * By default, the flag is not set.
	 * @param flag if true, the flag is set
	 */
	static public void setMinimizeAlways(boolean flag) {
		minimize_always = flag;
	}
	
	/**
	 * Sets or resets allow mutate flag.
	 * If this flag is set, then all automata operations may modify automata given as input;
	 * otherwise, operations will always leave input automata languages unmodified. 
	 * By default, the flag is not set.
	 * @param flag if true, the flag is set
	 * @return previous value of the flag
	 */
	static public boolean setAllowMutate(boolean flag) {
		boolean b = allow_mutation;
		allow_mutation = flag;
		return b;
	}
	
	/**
	 * Returns the state of the allow mutate flag.
	 * If this flag is set, then all automata operations may modify automata given as input;
	 * otherwise, operations will always leave input automata languages unmodified. 
	 * By default, the flag is not set.
	 * @return current value of the flag
	 */
	static boolean getAllowMutate() {
		return allow_mutation;
	}
	
	void checkMinimizeAlways() {
		if (minimize_always) {
			minimize();
		}
	}
	
	boolean isSingleton() {
		return _singleton !=null;
	}

	/**
	 * Returns the singleton string for this automaton.
	 * An automaton that accepts exactly one string <i>may</i> be represented
	 * in singleton mode. In that case, this method may be used to obtain the string.
	 * @return string, null if this automaton is not in singleton mode.
	 */
	public String getSingleton() {
		return _singleton;
	}
	
	/** 
	 * Sets initial state. 
	 * @param s state
	 */
	public void setInitialState(State s) {
		_initial = s;
		_singleton = null;
	}
	
	/** 
	 * Gets initial state. 
	 * @return state
	 */
	public State getInitialState() {
		expandSingleton();
		return _initial;
	}
	
	/**
	 * Returns deterministic flag for this automaton.
	 * @return true if the automaton is definitely deterministic, false if the automaton
	 *         may be nondeterministic
	 */
	public boolean isDeterministic() {
		return _deterministic;
	}
	
	/**
	 * Sets deterministic flag for this automaton.
	 * This method should (only) be used if automata are constructed manually.
	 * @param deterministic true if the automaton is definitely deterministic, false if the automaton
	 *                      may be nondeterministic
	 */
	public void setDeterministic(boolean deterministic) {
		this._deterministic = deterministic;
	}
	
	/**
	 * Associates extra information with this automaton. 
	 * @param info extra information
	 */
	public void setInfo(Object info) {
		this._info = info;
	}
	
	/**
	 * Returns extra information associated with this automaton. 
	 * @return extra information
	 * @see #setInfo(Object)
	 */
	public Object getInfo()	{
		return _info;
	}
	
	/** 
	 * Returns the set of states that are reachable from the initial state.
	 * @return set of {@link State} objects
	 */
	public Set<State> getStates() {
		expandSingleton();
		Set<State> visited;

		visited = new HashSet<>();
		LinkedList<State> worklist = new LinkedList<State>();
		worklist.add(_initial);
		visited.add(_initial);
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			Collection<Transition> tr;

			tr = s._transitionSet;
			for (Transition t : tr) {
				if (!visited.contains(t._to)) {
					visited.add(t._to);
					worklist.add(t._to);
				}
			}
		}
		return visited;
	}
	
	/** 
	 * Returns the set of reachable accept states. 
	 * @return set of {@link State} objects
	 */
	public Set<State> getAcceptStates() {
		expandSingleton();
		HashSet<State> accepts = new HashSet<State>();
		HashSet<State> visited = new HashSet<State>();
		LinkedList<State> worklist = new LinkedList<State>();
		worklist.add(_initial);
		visited.add(_initial);
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			if (s._accept) {
				accepts.add(s);
			}
			for (Transition t : s._transitionSet) {
				if (!visited.contains(t._to)) {
					visited.add(t._to);
					worklist.add(t._to);
				}
			}
		}
		return accepts;
	}
	
	/** 
	 * Assigns consecutive numbers to the given states. 
	 */
	static void setStateNumbers(Set<State> states) {
		if (states.size() == Integer.MAX_VALUE) {
			throw new IllegalArgumentException("number of states exceeded Integer.MAX_VALUE");
		}
		int number = 0;
		for (State s : states) {
			s._number = number++;
		}
	}
	
	/** 
	 * Adds transitions to explicit crash state to ensure that transition function is total. 
	 */
	void totalize() {
		State s = new State();
		s._transitionSet.add(new Transition(Character.MIN_VALUE, Character.MAX_VALUE, s));
		for (State p : getStates()) {
			int maxi = Character.MIN_VALUE;
			for (Transition t : p.getSortedTransitions(false)) {
				if (t._min > maxi) {
					p._transitionSet.add(new Transition((char) maxi, (char) (t._min - 1), s));
				}
				if (t._max + 1 > maxi) {
					maxi = t._max + 1;
				}
			}
			if (maxi <= Character.MAX_VALUE) {
				p._transitionSet.add(new Transition((char) maxi, Character.MAX_VALUE, s));
			}
		}
	}

	/**
	 * Restores representation invariant.
	 * This method must be invoked before any built-in automata operation is performed
	 * if automaton states or transitions are manipulated manually.
	 * @see #setDeterministic(boolean)
	 */
	public void restoreInvariant() {
		removeDeadTransitions();
	}
	
	/** 
	 * Reduces this automaton.
	 * An automaton is "reduced" by combining overlapping and adjacent edge intervals with same destination. 
	 */
	public void reduce() {
		if (isSingleton()) {
			return;
		}
		Set<State> states = getStates();
		setStateNumbers(states);
		for (State s : states) {
			List<Transition> st = s.getSortedTransitions(true);
			s.resetTransitions();
			State p = null;
			int min = -1, max = -1;
			for (Transition t : st) {
				if (p == t._to) {
					if (t._min <= max + 1) {
						if (t._max > max) {
							max = t._max;
						}
					} else {
						if (p != null) {
							s._transitionSet.add(new Transition((char) min, (char) max, p));
						}
						min = t._min;
						max = t._max;
					}
				} else {
					if (p != null) {
						s._transitionSet.add(new Transition((char) min, (char) max, p));
					}
					p = t._to;
					min = t._min;
					max = t._max;
				}
			}
			if (p != null) {
				s._transitionSet.add(new Transition((char) min, (char) max, p));
			}
		}
		clearHashCode();
	}
	
	/** 
	 * Returns sorted array of all interval start points. 
	 */
	char[] getStartPoints() {
		Set<Character> pointset = new HashSet<Character>();
		pointset.add(Character.MIN_VALUE);
		for (State s : getStates()) {
			for (Transition t : s._transitionSet) {
				pointset.add(t._min);
				if (t._max < Character.MAX_VALUE) {
					pointset.add((char) (t._max + 1));
				}
			}
		}
		char[] points = new char[pointset.size()];
		int n = 0;
		for (Character m : pointset) {
			points[n++] = m;
		}
		Arrays.sort(points);
		return points;
	}
	
	private Set<State> getLiveStates(Set<State> states) {
		HashMap<State, Set<State>> map = new HashMap<State, Set<State>>();
		for (State s : states) {
			map.put(s, new HashSet<>());
		}
		for (State s : states) {
			for (Transition t : s._transitionSet) {
				map.get(t._to).add(s);
			}
		}
		Set<State> live = new HashSet<State>(getAcceptStates());
		LinkedList<State> worklist = new LinkedList<State>(live);
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			for (State p : map.get(s)) {
				if (!live.contains(p)) {
					live.add(p);
					worklist.add(p);
				}
			}
		}
		return live;
	}
	
	/** 
	 * Removes transitions to dead states and calls {@link #reduce()} and {@link #clearHashCode()}.
	 * (A state is "dead" if no accept state is reachable from it.)
	 */
	public void removeDeadTransitions() {
		clearHashCode();
		if (isSingleton()) {
			return;
		}
		Set<State> states = getStates();
		Set<State> live = getLiveStates(states);
		for (State s : states) {
			Set<Transition> st = s._transitionSet;
			s.resetTransitions();
			for (Transition t : st) {
				if (live.contains(t._to)) {
					s._transitionSet.add(t);
				}
			}
		}
		reduce();
	}
	
	/** 
	 * Returns a sorted array of transitions for each state (and sets state numbers). 
	 */
	static Transition[][] getSortedTransitions(Set<State> states) {
		setStateNumbers(states);
		Transition[][] transitions = new Transition[states.size()][];
		for (State s : states) {
			transitions[s._number] = s.getSortedTransitionArray(false);
		}
		return transitions;
	}
	
	/** 
	 * Expands singleton representation to normal representation.
	 * Does nothing if not in singleton representation. 
	 */
	public void expandSingleton() {
		if (isSingleton()) {
			State p = new State();
			_initial = p;
			for (int i = 0; i < _singleton.length(); i++) {
				State q = new State();
				q._number = i;
				p._transitionSet.add(new Transition(_singleton.charAt(i), q));
				p = q;
			}
			p._accept = true;
			_deterministic = true;
			_singleton = null;
		}
	}
	
	/**
	 * Returns the number of states in this automaton.
	 */
	public int getNumberOfStates() {
		if (isSingleton()) {
			return _singleton.length() + 1;
		}
		return getStates().size();
	}
	
	/**
	 * Returns the number of transitions in this automaton. This number is counted
	 * as the total number of edges, where one edge may be a character interval.
	 */
	public int getNumberOfTransitions() {
		if (isSingleton()) {
			return _singleton.length();
		}
		int c = 0;
		for (State s : getStates()) {
			c += s._transitionSet.size();
		}
		return c;
	}
	
	/**
	 * Returns true if the language of this automaton is equal to the language
	 * of the given automaton. Implemented using <code>hashCode</code> and
	 * <code>subsetOf</code>.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof Automaton)) {
			return false;
		}
		Automaton a = (Automaton)obj;
		if (isSingleton() && a.isSingleton()) {
			return _singleton.equals(a._singleton);
		}
		return hashCode() == a.hashCode() && subsetOf(a) && a.subsetOf(this);
	}
	
	/**
	 * Returns hash code for this automaton. The hash code is based on the
	 * number of states and transitions in the minimized automaton.
	 * Invoking this method may involve minimizing the automaton.
	 */
	@Override
	public int hashCode() {
		if (_hashCode == 0) {
			minimize();
		}
		return _hashCode;
	}
	
	/**
	 * Recomputes the hash code.
	 * The automaton must be minimal when this operation is performed.
	 */
	void recomputeHashCode() {
		_hashCode = getNumberOfStates() * 3 + getNumberOfTransitions() * 2;
		if (_hashCode == 0) {
			_hashCode = 1;
		}
	}
	
	/**
	 * Must be invoked when the stored hash code may no longer be valid.
	 */
	void clearHashCode() {
		_hashCode = 0;
	}
	
	/**
	 * Returns a string representation of this automaton.
	 */
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		if (isSingleton()) {
			b.append("singleton: ");
			for (char c : _singleton.toCharArray()) {
				Transition.appendCharString(c, b);
			}
			b.append("\n");
		} else {
			Set<State> states = getStates();
			setStateNumbers(states);
			b.append("initial state: ").append(_initial._number).append("\n");
			for (State s : states) {
				b.append(s.toString());
			}
		}
		return b.toString();
	}
	
	/**
	 * Returns a clone of this automaton, expands if singleton.
	 */
	Automaton cloneExpanded() {
		Automaton a = clone();
		a.expandSingleton();
		return a;
	}

	/**
	 * Returns a clone of this automaton unless <code>allow_mutation</code> is set, expands if singleton.
	 */
	Automaton cloneExpandedIfRequired() {
		if (allow_mutation) {
			expandSingleton();
			return this;
		} else {
			return cloneExpanded();
		}
	}

	/**
	 * Returns a clone of this automaton.
	 */
	@Override
	public Automaton clone() {
		try {
			Automaton a = (Automaton)super.clone();
			if (!isSingleton()) {
				HashMap<State, State> m = new HashMap<State, State>();
				Set<State> states = getStates();
				for (State s : states) {
					m.put(s, new State());
				}
				for (State s : states) {
					State p = m.get(s);
					p._accept = s._accept;
					if (s == _initial) {
						a._initial = p;
					}
					for (Transition t : s._transitionSet) {
						p._transitionSet.add(new Transition(t._min, t._max, m.get(t._to)));
					}
				}
			}
			return a;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Returns a clone of this automaton, or this automaton itself if <code>allow_mutation</code> flag is set. 
	 */
	Automaton cloneIfRequired() {
		if (allow_mutation) {
			return this;
		} else {
			return clone();
		}
	}
	
	/** 
	 * Retrieves a serialized <code>Automaton</code> located by a URL.
	 * @param url URL of serialized automaton
	 * @exception IOException if input/output related exception occurs
	 * @exception ClassCastException if the data is not a serialized <code>Automaton</code>
	 * @exception ClassNotFoundException if the class of the serialized object cannot be found
	 */
	public static Automaton load(URL url) throws IOException, ClassCastException, ClassNotFoundException {
		return load(url.openStream());
	}
	
	/**
	 * Retrieves a serialized <code>Automaton</code> from a stream.
	 * @param stream input stream with serialized automaton
	 * @exception IOException if input/output related exception occurs
	 * @exception ClassCastException if the data is not a serialized <code>Automaton</code>
	 * @exception ClassNotFoundException if the class of the serialized object cannot be found
	 */
	public static Automaton load(InputStream stream) throws IOException, ClassCastException, ClassNotFoundException {
		ObjectInputStream s = new ObjectInputStream(stream);
		return (Automaton)s.readObject();
	}
	
	/**
	 * Writes this <code>Automaton</code> to the given stream.
	 * @param stream output stream for serialized automaton
	 * @exception IOException if input/output related exception occurs
	 */
	public void store(OutputStream stream) throws IOException {
		ObjectOutputStream s = new ObjectOutputStream(stream);
		s.writeObject(this);
		s.flush();
	}

	/** 
	 * See {@link BasicAutomata#makeChar(char)}.
	 */
	public static Automaton makeChar(char c) {
		return BasicAutomata.makeChar(c);
	}
	
	/** 
	 * See {@link BasicAutomata#makeCharRange(char, char)}.
	 */
	public static Automaton makeCharRange(char min, char max) {
		return BasicAutomata.makeCharRange(min, max);
	}
	
	/** 
	 * See {@link BasicAutomata#makeCharSet(String)}.
	 */
	public static Automaton makeCharSet(String set) {
		return BasicAutomata.makeCharSet(set);
	}
	
	/** 
	 * See {@link BasicAutomata#makeString(String)}.
	 */
	public static Automaton makeString(String s) {
		return BasicAutomata.makeString(s);
	}

	/**
	 * See {@link BasicAutomata#makeMaxInteger(String)}.
	 */
	public static Automaton makeMaxInteger(String n) {
		return BasicAutomata.makeMaxInteger(n);
	}

	/** 
	 * See {@link BasicOperations#concatenate(Automaton, Automaton)}.
	 */
	public Automaton concatenate(Automaton a) {
		return BasicOperations.concatenate(this, a);
	}
	
	/**
	 * See {@link BasicOperations#concatenate(List)}.
	 */
	static public Automaton concatenate(List<Automaton> l) {
		return BasicOperations.concatenate(l);
	}

	/**
	 * See {@link BasicOperations#optional(Automaton)}.
	 */
	public Automaton optional() {
		return BasicOperations.optional(this);
	}
	
	/**
	 * See {@link BasicOperations#repeat(Automaton)}.
	 */
	public Automaton repeat() {
		return BasicOperations.repeat(this);
	}

	/**
	 * See {@link BasicOperations#repeat(Automaton, int)}.
	 */
	public Automaton repeat(int min) {
		return BasicOperations.repeat(this, min);
	}
	
	/**
	 * See {@link BasicOperations#repeat(Automaton, int, int)}.
	 */
	public Automaton repeat(int min, int max) {
		return BasicOperations.repeat(this, min, max);
	}

	/**
	 * See {@link BasicOperations#complement(Automaton)}.
	 */
	public Automaton complement() {
		return BasicOperations.complement(this);
	}

	/**
	 * See {@link BasicOperations#minus(Automaton, Automaton)}.
	 */
	public Automaton minus(Automaton a) {
		return BasicOperations.minus(this, a);
	}

	/**
	 * See {@link BasicOperations#intersection(Automaton, Automaton)}.
	 */
	public Automaton intersection(Automaton a) {
		return BasicOperations.intersection(this, a);
	}
	
	/**
	 * See {@link BasicOperations#subsetOf(Automaton, Automaton)}.
	 */
	public boolean subsetOf(Automaton a) {
		return BasicOperations.subsetOf(this, a);
	}
	
	/**
	 * See {@link BasicOperations#union(Automaton, Automaton)}.
	 */
	public Automaton union(Automaton a) {
		return BasicOperations.union(this, a);
	}
	
	/**
	 * See {@link BasicOperations#union(Collection)}.
	 */
	static public Automaton union(Collection<Automaton> l) {
		return BasicOperations.union(l);
	}

	/**
	 * See {@link BasicOperations#determinize(Automaton)}.
	 */
	public void determinize() {
		BasicOperations.determinize(this);
	}

	/** 
	 * See {@link BasicOperations#addEpsilons(Automaton, Collection)}.
	 */
	public void addEpsilons(Collection<StatePair> pairs) {
		BasicOperations.addEpsilons(this, pairs);
	}
	
	/**
	 * See {@link BasicOperations#isEmptyString(Automaton)}.
	 */
	public boolean isEmptyString() {
		return BasicOperations.isEmptyString(this);
	}

	/**
	 * See {@link BasicOperations#isEmpty(Automaton)}.
	 */
	public boolean isEmpty() {
		return BasicOperations.isEmpty(this);
	}
	
	/**
	 * See {@link BasicOperations#isTotal(Automaton)}.
	 */
	public boolean isTotal() {
		return BasicOperations.isTotal(this);
	}
	
	/**
	 * See {@link BasicOperations#run(Automaton, String)}.
	 */
	public boolean run(String s) {
		return BasicOperations.run(this, s);
	}
	
	/**
	 * See {@link MinimizationOperations#minimize(Automaton)}.
	 */
	public void minimize() {
		MinimizationOperations.minimize(this);
	}
	
	/**
	 * See {@link MinimizationOperations#minimize(Automaton)}.
	 * Returns the automaton being given as argument.
	 */
	public static Automaton minimize(Automaton a) {
		a.minimize();
		return a;
	}
	
	/**
	 * See {@link SpecialOperations#overlap(Automaton, Automaton)}.
	 */
	public Automaton overlap(Automaton a) {
		return SpecialOperations.overlap(this, a);
	}
	
	/**
	 * See {@link SpecialOperations#trim(Automaton, String, char)}.
	 */
	public Automaton trim(String set, char c) {
		return SpecialOperations.trim(this, set, c);
	}
	
	/**
	 * See {@link SpecialOperations#compress(Automaton, String, char)}.
	 */
	public Automaton compress(String set, char c) {
		return SpecialOperations.compress(this, set, c);
	}
	
	/**
	 * See {@link SpecialOperations#subst(Automaton, Map)}.
	 */
	public Automaton subst(Map<Character,Set<Character>> map) {
		return SpecialOperations.subst(this, map);
	}

	/**
	 * See {@link SpecialOperations#subst(Automaton, char, String)}.
	 */
	public Automaton subst(char c, String s) {
		return SpecialOperations.subst(this, c, s);
	}
	
	/**
	 * See {@link SpecialOperations#isFinite(Automaton)}.
	 */
	public boolean isFinite() {
		return SpecialOperations.isFinite(this);
	}

	/**
	 * See {@link SpecialOperations#getCommonPrefix(Automaton)}.
	 */
	public String getCommonPrefix() {
		return SpecialOperations.getCommonPrefix(this);
	}

	/** 
	 * See {@link ShuffleOperations#shuffle(Automaton, Automaton)}.
	 */
	public Automaton shuffle(Automaton a) {
		return ShuffleOperations.shuffle(this, a);
	}
}
