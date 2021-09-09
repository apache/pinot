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

/** 
 * <tt>Automaton</tt> transition. 
 * <p>
 * A transition, which belongs to a source state, consists of a Unicode character interval
 * and a destination state.
 */
public class Transition implements Serializable, Cloneable {
	
	static final long serialVersionUID = 40001;
	
	/* 
	 * CLASS INVARIANT: min<=max
	 */
	
	public  char _min;
	public char _max;
	
	public State _to;
	
	/** 
	 * Constructs a new singleton interval transition. 
	 * @param c transition character
	 * @param to destination state
	 */
	public Transition(char c, State to)	{
		_min = _max = c;
		this._to = to;
	}
	
	/** 
	 * Constructs a new transition. 
	 * Both end points are included in the interval.
	 * @param min transition interval minimum
	 * @param max transition interval maximum
	 * @param to destination state
	 */
	public Transition(char min, char max, State to)	{
		if (max < min) {
			char t = max;
			max = min;
			min = t;
		}
		this._min = min;
		this._max = max;
		this._to = to;
	}
	
	/** Returns minimum of this transition interval. */
	public char getMin() {
		return _min;
	}
	
	/** Returns maximum of this transition interval. */
	public char getMax() {
		return _max;
	}
	
	/** Returns destination of this transition. */
	public State getDest() {
		return _to;
	}
	
	/** 
	 * Checks for equality.
	 * @param obj object to compare with
	 * @return true if <tt>obj</tt> is a transition with same 
	 *         character interval and destination state as this transition.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Transition) {
			Transition t = (Transition)obj;
			return t._min == _min && t._max == _max && t._to == _to;
		} else {
      return false;
    }
	}
	
	/** 
	 * Returns hash code.
	 * The hash code is based on the character interval (not the destination state).
	 * @return hash code
	 */
	@Override
	public int hashCode() {
		return _min * 2 + _max * 3;
	}
	
	/** 
	 * Clones this transition. 
	 * @return clone with same character interval and destination state
	 */
	@Override
	public Transition clone() {
		try {
			return (Transition)super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
	static void appendCharString(char c, StringBuilder b) {
		if (c >= 0x21 && c <= 0x7e && c != '\\' && c != '"') {
      b.append(c);
    } else {
			b.append("\\u");
			String s = Integer.toHexString(c);
			if (c < 0x10) {
        b.append("000").append(s);
      } else if (c < 0x100) {
        b.append("00").append(s);
      } else if (c < 0x1000) {
        b.append("0").append(s);
      } else {
        b.append(s);
      }
		}
	}
	
	/** 
	 * Returns a string describing this state. Normally invoked via 
	 * {@link Automaton#toString()}. 
	 */
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		appendCharString(_min, b);
		if (_min != _max) {
			b.append("-");
			appendCharString(_max, b);
		}
		b.append(" -> ").append(_to._number);
		return b.toString();
	}

	void appendDot(StringBuilder b) {
		b.append(" -> ").append(_to._number).append(" [label=\"");
		appendCharString(_min, b);
		if (_min != _max) {
			b.append("-");
			appendCharString(_max, b);
		}
		b.append("\"]\n");
	}
}
