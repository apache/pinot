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

package org.apache.pinot.fsa.automaton;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Set;

/**
 * Finite-state automaton with fast run operation.
 */
public class RunAutomaton implements Serializable {

	static final long serialVersionUID = 20001;

	int size;
	boolean[] accept;
	int initial;
	int[] transitions; // delta(state,c) = transitions[state*points.length + getCharClass(c)]
	char[] points; // char interval start points
	int[] classmap; // map from char number to class class

	final int alphabetSize;

	/** 
	 * Sets alphabet table for optimal run performance. 
	 */
	void setAlphabet() {
		classmap = new int[Character.MAX_VALUE - Character.MIN_VALUE + 1];
		int i = 0;
		for (int j = 0; j <= Character.MAX_VALUE - Character.MIN_VALUE; j++) {
			if (i + 1 < points.length && j == points[i + 1])
				i++;
			classmap[j] = i;
		}
	}

	/** 
	 * Returns a string representation of this automaton. 
	 */
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("initial state: ").append(initial).append("\n");
		for (int i = 0; i < size; i++) {
			b.append("state ").append(i);
			if (accept[i])
				b.append(" [accept]:\n");
			else
				b.append(" [reject]:\n");
			for (int j = 0; j < points.length; j++) {
				int k = transitions[i * points.length + j];
				if (k != -1) {
					char min = points[j];
					char max;
					if (j + 1 < points.length)
						max = (char)(points[j + 1] - 1);
					else
						max = Character.MAX_VALUE;
					b.append(" ");
					Transition.appendCharString(min, b);
					if (min != max) {
						b.append("-");
						Transition.appendCharString(max, b);
					}
					b.append(" -> ").append(k).append("\n");
				}
			}
		}
		return b.toString();
	}

	/** 
	 * Returns number of states in automaton. 
	 */
	public int getSize() {
		return size;
	}

	/** 
	 * Returns acceptance status for given state. 
	 */
	public boolean isAccept(int state) {
		return accept[state];
	}

	/** 
	 * Returns initial state. 
	 */
	public int getInitialState() {
		return initial;
	}

	/**
	 * Returns array of character class interval start points. The array should
	 * not be modified by the caller.
	 */
	public char[] getCharIntervals() {
		return points.clone();
	}

	/** 
	 * Gets character class of given char. 
	 */
	int getCharClass(char c) {
		return SpecialOperations.findIndex(c, points);
	}

	/**
	 * Constructs a new <code>RunAutomaton</code> from a deterministic
	 * <code>Automaton</code>. Same as <code>RunAutomaton(a, true)</code>.
	 * @param a an automaton
	 */
	public RunAutomaton(Automaton a, int alphabetSize) {
		this(a, true, alphabetSize);
	}

	/**
	 * Retrieves a serialized <code>RunAutomaton</code> located by a URL.
	 * @param url URL of serialized automaton
	 * @exception IOException if input/output related exception occurs
	 * @exception ClassCastException if the data is not a serialized <code>RunAutomaton</code>
	 * @exception ClassNotFoundException if the class of the serialized object cannot be found
	 */
	public static RunAutomaton load(URL url) throws IOException, ClassCastException, ClassNotFoundException {
		return load(url.openStream());
	}

	/**
	 * Retrieves a serialized <code>RunAutomaton</code> from a stream.
	 * @param stream input stream with serialized automaton
	 * @exception IOException if input/output related exception occurs
	 * @exception ClassCastException if the data is not a serialized <code>RunAutomaton</code>
	 * @exception ClassNotFoundException if the class of the serialized object cannot be found
	 */
	public static RunAutomaton load(InputStream stream) throws IOException, ClassCastException, ClassNotFoundException {
		ObjectInputStream s = new ObjectInputStream(stream);
		return (RunAutomaton) s.readObject();
	}

	/**
	 * Writes this <code>RunAutomaton</code> to the given stream.
	 * @param stream output stream for serialized automaton
	 * @exception IOException if input/output related exception occurs
	 */
	public void store(OutputStream stream) throws IOException {
		ObjectOutputStream s = new ObjectOutputStream(stream);
		s.writeObject(this);
		s.flush();
	}

	/**
	 * Constructs a new <code>RunAutomaton</code> from a deterministic
	 * <code>Automaton</code>. If the given automaton is not deterministic,
	 * it is determinized first.
	 * @param a an automaton
	 * @param tableize if true, a transition table is created which makes the <code>run</code> 
	 *                 method faster in return of a higher memory usage
	 */
	public RunAutomaton(Automaton a, boolean tableize, int alphabetSize) {
		this.alphabetSize = alphabetSize;

		a.determinize();
		points = a.getStartPoints();
		Set<State> states = a.getStates();
		Automaton.setStateNumbers(states);
		initial = a.initial.number;
		size = states.size();
		accept = new boolean[size];
		transitions = new int[size * points.length];
		for (int n = 0; n < size * points.length; n++)
			transitions[n] = -1;
		for (State s : states) {
			int n = s.number;
			accept[n] = s.accept;
			for (int c = 0; c < points.length; c++) {
				State q = s.step(points[c]);
				if (q != null)
					transitions[n * points.length + c] = q.number;
			}
		}
		if (tableize)
			setAlphabet();
	}

	/** Gets character class of given codepoint */
	final int getCharClass(int c) {

		// binary search
		int a = 0;
		int b = points.length;
		while (b - a > 1) {
			int d = (a + b) >>> 1;
			if (points[d] > c) b = d;
			else if (points[d] < c) a = d;
			else return d;
		}
		return a;
	}

	/**
	 * Returns the state obtained by reading the given char from the given state. Returns -1 if not
	 * obtaining any such state. (If the original <code>Automaton</code> had no dead states, -1 is
	 * returned here if and only if a dead state is entered in an equivalent automaton with a total
	 * transition function.)
	 */
	public final int step(int state, int c) {
		assert c < alphabetSize;
		if (c >= classmap.length) {
			return transitions[state * points.length + getCharClass(c)];
		} else {
			return transitions[state * points.length + classmap[c]];
		}
	}

		/**
     * Returns the state obtained by reading the given char from the given
     * state. Returns -1 if not obtaining any such state. (If the original
     * <code>Automaton</code> had no dead states, -1 is returned here if and
     * only if a dead state is entered in an equivalent automaton with a total
     * transition function.)
     */
	public int step(int state, char c) {
		if (classmap == null)
			return transitions[state * points.length + getCharClass(c)];
		else
			return transitions[state * points.length + classmap[c - Character.MIN_VALUE]];
	}

	/** 
	 * Returns true if the given string is accepted by this automaton. 
	 */
	public boolean run(String s) {
		int p = initial;
		int l = s.length();
		for (int i = 0; i < l; i++) {
			p = step(p, s.charAt(i));
			if (p == -1)
				return false;
		}
		return accept[p];
	}

	/**
	 * Returns the length of the longest accepted run of the given string
	 * starting at the given offset.
	 * @param s the string
	 * @param offset offset into <code>s</code> where the run starts
	 * @return length of the longest accepted run, -1 if no run is accepted
	 */
	public int run(String s, int offset) {
		int p = initial;
		int l = s.length();
		int max = -1;
		for (int r = 0; offset <= l; offset++, r++) {
			if (accept[p])
				max = r;
			if (offset == l)
				break;
			p = step(p, s.charAt(offset));
			if (p == -1)
				break;
		}
		return max;
	}

	/**
	 * Creates a new automaton matcher for the given input.
	 * @param s the CharSequence to search
	 * @return A new automaton matcher for the given input
	 */
	public AutomatonMatcher newMatcher(CharSequence s)  {
		return new AutomatonMatcher(s, this);
	}

	/**
	 * Creates a new automaton matcher for the given input.
	 * @param s the CharSequence to search
	 * @param startOffset the starting offset of the given character sequence
	 * @param endOffset the ending offset of the given character sequence
	 * @return A new automaton matcher for the given input
	 */
	public AutomatonMatcher newMatcher(CharSequence s, int startOffset, int endOffset)  {
		return new AutomatonMatcher(s.subSequence(startOffset, endOffset), this);
	}
}
