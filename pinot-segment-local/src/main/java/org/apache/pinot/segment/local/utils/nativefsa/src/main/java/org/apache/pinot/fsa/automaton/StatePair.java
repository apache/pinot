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

package org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.automaton;

/**
 * Pair of states.
 */
public class StatePair {
	State s;
	State s1;
	State s2;
	
	StatePair(State s, State s1, State s2) {
		this.s = s;
		this.s1 = s1;
		this.s2 = s2;
	}
	
	/**
	 * Constructs a new state pair.
	 * @param s1 first state
	 * @param s2 second state
	 */
	public StatePair(State s1, State s2) {
		this.s1 = s1;
		this.s2 = s2;
	}
	
	/**
	 * Returns first component of this pair.
	 * @return first state
	 */
	public State getFirstState() {
		return s1;
	}
	
	/**
	 * Returns second component of this pair.
	 * @return second state
	 */
	public State getSecondState() {
		return s2;
	}
	
	/** 
	 * Checks for equality.
	 * @param obj object to compare with
	 * @return true if <tt>obj</tt> represents the same pair of states as this pair
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StatePair) {
			StatePair p = (StatePair)obj;
			return p.s1 == s1 && p.s2 == s2;
		}
		else
			return false;
	}
	
	/** 
	 * Returns hash code.
	 * @return hash code
	 */
	@Override
	public int hashCode() {
		return s1.hashCode() + s2.hashCode();
	}
}
