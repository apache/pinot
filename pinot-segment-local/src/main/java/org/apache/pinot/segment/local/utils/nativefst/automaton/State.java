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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 
 * <tt>Automaton</tt> state.
 */
public class State implements Serializable, Comparable<State> {
	
	static final long serialVersionUID = 30001;
	static int next_id;
	
	boolean _accept;
	Set<Transition> _transitionSet;
	
	int _number;
	
	int _id;
	
	/** 
	 * Constructs a new state. Initially, the new state is a reject state. 
	 */
	public State() {
		resetTransitions();
		_id = next_id++;
	}
	
	/** 
	 * Resets transition set. 
	 */
	final void resetTransitions() {
		_transitionSet = new HashSet<Transition>();
	}
	
	/** 
	 * Returns the set of outgoing transitions. 
	 * Subsequent changes are reflected in the automaton.
	 * @return transition set
	 */
	public Set<Transition> getTransitionSet()	{
		return _transitionSet;
	}
	
	/**
	 * Adds an outgoing transition.
	 * @param t transition
	 */
	public void addTransition(Transition t)	{
		_transitionSet.add(t);
	}
	
	/** 
	 * Sets acceptance for this state.
	 * @param accept if true, this state is an accept state
	 */
	public void setAccept(boolean accept) {
		this._accept = accept;
	}
	
	/**
	 * Returns acceptance status.
	 * @return true is this is an accept state
	 */
	public boolean isAccept() {
		return _accept;
	}
	
	/** 
	 * Performs lookup in transitions, assuming determinism. 
	 * @param c character to look up
	 * @return destination state, null if no matching outgoing transition
	 * @see #step(char, Collection)
	 */
	public State step(char c) {
		for (Transition t : _transitionSet) {
      if (t._min <= c && c <= t._max) {
        return t._to;
      }
    }
		return null;
	}

	/** 
	 * Performs lookup in transitions, allowing nondeterminism.
	 * @param c character to look up
	 * @param dest collection where destination states are stored
	 * @see #step(char)
	 */
	public void step(char c, Collection<State> dest) {
		for (Transition t : _transitionSet) {
      if (t._min <= c && c <= t._max) {
        dest.add(t._to);
      }
    }
	}

	void addEpsilon(State to) {
		if (to._accept) {
      _accept = true;
    }
		_transitionSet.addAll(to._transitionSet);
	}
	
	/** Returns transitions sorted by (min, reverse max, to) or (to, min, reverse max) */
	Transition[] getSortedTransitionArray(boolean to_first) {
		Transition[] e = _transitionSet.toArray(new Transition[_transitionSet.size()]);
		Arrays.sort(e, new TransitionComparator(to_first));
		return e;
	}
	
	/**
	 * Returns sorted list of outgoing transitions.
	 * @param to_first if true, order by (to, min, reverse max); otherwise (min, reverse max, to)
	 * @return transition list
	 */
	public List<Transition> getSortedTransitions(boolean to_first)	{
		return Arrays.asList(getSortedTransitionArray(to_first));
	}
	
	/** 
	 * Returns string describing this state. Normally invoked via 
	 * {@link Automaton#toString()}. 
	 */
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("state ").append(_number);
		if (_accept) {
      b.append(" [accept]");
    } else {
      b.append(" [reject]");
    }
		b.append(":\n");
		for (Transition t : _transitionSet) {
      b.append("  ").append(t.toString()).append("\n");
    }
		return b.toString();
	}
	
	/**
	 * Compares this object with the specified object for order.
	 * States are ordered by the time of construction.
	 */
	public int compareTo(State s) {
		return s._id - _id;
	}

	/**
	 * See {@link Object#equals(Object)}.
	 */
	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}

	/**
	 * See {@link Object#hashCode()}.
	 */
	@Override
	public int hashCode() {
		return super.hashCode();
	}
}
