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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Regular Expression extension to <code>Automaton</code>.
 * <p>
 * Regular expressions are built from the following abstract syntax:
 * <table border=0>
 * <tr><td><i>regexp</i></td><td>::=</td><td><i>unionexp</i></td><td></td><td></td></tr>
 * <tr><td></td><td>|</td><td></td><td></td><td></td></tr>
 *
 * <tr><td><i>unionexp</i></td><td>::=</td><td><i>interexp</i>&nbsp;<tt><b>|</b></tt>&nbsp;<i>unionexp</i></td><td>(union)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>interexp</i></td><td></td><td></td></tr>
 *
 * <tr><td><i>interexp</i></td><td>::=</td><td><i>concatexp</i>&nbsp;<tt><b>&amp;</b></tt>&nbsp;<i>interexp</i></td><td>(intersection)</td><td><small>[OPTIONAL]</small></td></tr>
 * <tr><td></td><td>|</td><td><i>concatexp</i></td><td></td><td></td></tr>
 *
 * <tr><td><i>concatexp</i></td><td>::=</td><td><i>repeatexp</i>&nbsp;<i>concatexp</i></td><td>(concatenation)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>repeatexp</i></td><td></td><td></td></tr>
 *
 * <tr><td><i>repeatexp</i></td><td>::=</td><td><i>repeatexp</i>&nbsp;<tt><b>?</b></tt></td><td>(zero or one occurrence)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>repeatexp</i>&nbsp;<tt><b>*</b></tt></td><td>(zero or more occurrences)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>repeatexp</i>&nbsp;<tt><b>+</b></tt></td><td>(one or more occurrences)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>repeatexp</i>&nbsp;<tt><b>{</b><i>n</i><b>}</b></tt></td><td>(<tt><i>n</i></tt> occurrences)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>repeatexp</i>&nbsp;<tt><b>{</b><i>n</i><b>,}</b></tt></td><td>(<tt><i>n</i></tt> or more occurrences)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>repeatexp</i>&nbsp;<tt><b>{</b><i>n</i><b>,</b><i>m</i><b>}</b></tt></td><td>(<tt><i>n</i></tt> to <tt><i>m</i></tt> occurrences, including both)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>complexp</i></td><td></td><td></td></tr>
 *
 * <tr><td><i>complexp</i></td><td>::=</td><td><tt><b>~</b></tt>&nbsp;<i>complexp</i></td><td>(complement)</td><td><small>[OPTIONAL]</small></td></tr>
 * <tr><td></td><td>|</td><td><i>charclassexp</i></td><td></td><td></td></tr>
 *
 * <tr><td><i>charclassexp</i></td><td>::=</td><td><tt><b>[</b></tt>&nbsp;<i>charclasses</i>&nbsp;<tt><b>]</b></tt></td><td>(character class)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><tt><b>[^</b></tt>&nbsp;<i>charclasses</i>&nbsp;<tt><b>]</b></tt></td><td>(negated character class)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>simpleexp</i></td><td></td><td></td></tr>
 *
 * <tr><td><i>charclasses</i></td><td>::=</td><td><i>charclass</i>&nbsp;<i>charclasses</i></td><td></td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>charclass</i></td><td></td><td></td></tr>
 *
 * <tr><td><i>charclass</i></td><td>::=</td><td><i>charexp</i>&nbsp;<tt><b>-</b></tt>&nbsp;<i>charexp</i></td><td>(character range, including end-points)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><i>charexp</i></td><td></td><td></td></tr>
 *
 * <tr><td><i>simpleexp</i></td><td>::=</td><td><i>charexp</i></td><td></td><td></td></tr>
 * <tr><td></td><td>|</td><td><tt><b>.</b></tt></td><td>(any single character)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><tt><b>#</b></tt></td><td>(the empty language)</td><td><small>[OPTIONAL]</small></td></tr>
 * <tr><td></td><td>|</td><td><tt><b>@</b></tt></td><td>(any string)</td><td><small>[OPTIONAL]</small></td></tr>
 * <tr><td></td><td>|</td><td><tt><b>"</b></tt>&nbsp;&lt;Unicode string without double-quotes&gt;&nbsp;<tt><b>"</b></tt></td><td>(a string)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><tt><b>(</b></tt>&nbsp;<tt><b>)</b></tt></td><td>(the empty string)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><tt><b>(</b></tt>&nbsp;<i>unionexp</i>&nbsp;<tt><b>)</b></tt></td><td>(precedence override)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><tt><b>&lt;</b></tt>&nbsp;&lt;identifier&gt;&nbsp;<tt><b>&gt;</b></tt></td><td>(named automaton)</td><td><small>[OPTIONAL]</small></td></tr>
 * <tr><td></td><td>|</td><td><tt><b>&lt;</b><i>n</i>-<i>m</i><b>&gt;</b></tt></td><td>(numerical interval)</td><td><small>[OPTIONAL]</small></td></tr>
 *
 * <tr><td><i>charexp</i></td><td>::=</td><td>&lt;Unicode character&gt;</td><td>(a single non-reserved character)</td><td></td></tr>
 * <tr><td></td><td>|</td><td><tt><b>\</b></tt>&nbsp;&lt;Unicode character&gt;&nbsp;</td><td>(a single character)</td><td></td></tr>
 * </table>
 * <p>
 * The productions marked <small>[OPTIONAL]</small> are only allowed
 * if specified by the syntax flags passed to the <code>RegExp</code>
 * constructor.  The reserved characters used in the (enabled) syntax
 * must be escaped with backslash (<tt><b>\</b></tt>) or double-quotes
 * (<tt><b>"..."</b></tt>). (In contrast to other regexp syntaxes,
 * this is required also in character classes.)  Be aware that
 * dash (<tt><b>-</b></tt>) has a special meaning in <i>charclass</i> expressions.
 * An identifier is a string not containing right angle bracket
 * (<tt><b>&gt;</b></tt>) or dash (<tt><b>-</b></tt>).  Numerical intervals are
 * specified by non-negative decimal integers and include both end
 * points, and if <tt><i>n</i></tt> and <tt><i>m</i></tt> have the
 * same number of digits, then the conforming strings must have that
 * length (i.e. prefixed by 0's).
 */
public class RegExp {
	
	enum Kind {
		REGEXP_UNION,
		REGEXP_CONCATENATION,
		REGEXP_INTERSECTION,
		REGEXP_OPTIONAL,
		REGEXP_REPEAT,
		REGEXP_REPEAT_MIN,
		REGEXP_REPEAT_MINMAX,
		REGEXP_COMPLEMENT,
		REGEXP_CHAR,
		REGEXP_CHAR_RANGE,
		REGEXP_ANYCHAR,
		REGEXP_EMPTY,
		REGEXP_STRING,
		REGEXP_ANYSTRING,
		REGEXP_AUTOMATON,
		REGEXP_INTERVAL
	}
	
	/** 
	 * Syntax flag, enables intersection (<tt>&amp;</tt>). 
	 */
	public static final int INTERSECTION = 0x0001;
	
	/** 
	 * Syntax flag, enables complement (<tt>~</tt>). 
	 */
	public static final int COMPLEMENT = 0x0002;
	
	/** 
	 * Syntax flag, enables empty language (<tt>#</tt>). 
	 */
	public static final int EMPTY = 0x0004;
	
	/** 
	 * Syntax flag, enables anystring (<tt>@</tt>). 
	 */
	public static final int ANYSTRING = 0x0008;
	
	/** 
	 * Syntax flag, enables named automata (<tt>&lt;</tt>identifier<tt>&gt;</tt>). 
	 */
	public static final int AUTOMATON = 0x0010;
	
	/** 
	 * Syntax flag, enables numerical intervals (<tt>&lt;<i>n</i>-<i>m</i>&gt;</tt>). 
	 */
	public static final int INTERVAL = 0x0020;
	
	/** 
	 * Syntax flag, enables all optional regexp syntax. 
	 */
	public static final int ALL = 0xffff;
	
	/** 
	 * Syntax flag, enables no optional regexp syntax. 
	 */
	public static final int NONE = 0x0000;
	
	private static boolean allow_mutation = false;
	
	Kind _kind;
	RegExp _exp1, _exp2;
	String _processedString;
	char _char;
	int _min, _max, _digits;
	char _from, _to;
	
	String _inputString;
	int _flags;
	int _pos;

	RegExp() {}
	
	/** 
	 * Constructs new <code>RegExp</code> from a string. 
	 * Same as <code>RegExp(s, ALL)</code>.
	 * @param _inputString regexp string
	 * @exception IllegalArgumentException if an error occured while parsing the regular expression
	 */
	public RegExp(String _inputString) throws IllegalArgumentException {
		this(_inputString, ALL);
	}
	
	/** 
	 * Constructs new <code>RegExp</code> from a string. 
	 * @param _inputString regexp string
	 * @param syntax_flags boolean 'or' of optional syntax constructs to be enabled
	 * @exception IllegalArgumentException if an error occured while parsing the regular expression
	 */
	public RegExp(String _inputString, int syntax_flags) throws IllegalArgumentException {
		this._inputString = _inputString;
		_flags = syntax_flags;
		RegExp e;
		if (_inputString.length() == 0)
			e = makeString("");
		else {
			e = parseUnionExp();
			if (_pos < this._inputString.length())
				throw new IllegalArgumentException("end-of-string expected at position " + _pos);
		}
		_kind = e._kind;
		_exp1 = e._exp1;
		_exp2 = e._exp2;
		this._processedString = e._processedString;
		_char = e._char;
		_min = e._min;
		_max = e._max;
		_digits = e._digits;
		_from = e._from;
		_to = e._to;
		this._inputString = null;
	}
	
	/** 
	 * Constructs new <code>Automaton</code> from this <code>RegExp</code>. 
	 * Same as <code>toAutomaton(null)</code> (empty automaton map).
	 */
	public Automaton toAutomaton() {
		return toAutomatonAllowMutate(null, null, true);
	}
	
	/** 
	 * Constructs new <code>Automaton</code> from this <code>RegExp</code>. 
	 * Same as <code>toAutomaton(null,minimize)</code> (empty automaton map).
	 */
	public Automaton toAutomaton(boolean minimize) {
		return toAutomatonAllowMutate(null, null, minimize);
	}
	
	/** 
	 * Constructs new <code>Automaton</code> from this <code>RegExp</code>. 
	 * The constructed automaton is minimal and deterministic and has no 
	 * transitions to dead states. 
	 * @param automaton_provider provider of automata for named identifiers
	 * @exception IllegalArgumentException if this regular expression uses
	 *   a named identifier that is not available from the automaton provider
	 */
	public Automaton toAutomaton(AutomatonProvider automaton_provider) throws IllegalArgumentException {
		return toAutomatonAllowMutate(null, automaton_provider, true);
	}
		
	/** 
	 * Constructs new <code>Automaton</code> from this <code>RegExp</code>. 
	 * The constructed automaton has no transitions to dead states. 
	 * @param automaton_provider provider of automata for named identifiers
	 * @param minimize if set, the automaton is minimized and determinized
	 * @exception IllegalArgumentException if this regular expression uses
	 *   a named identifier that is not available from the automaton provider
	 */
	public Automaton toAutomaton(AutomatonProvider automaton_provider, boolean minimize) throws IllegalArgumentException {
		return toAutomatonAllowMutate(null, automaton_provider, minimize);
	}
		
	/** 
	 * Constructs new <code>Automaton</code> from this <code>RegExp</code>. 
	 * The constructed automaton is minimal and deterministic and has no 
	 * transitions to dead states. 
	 * @param automata a map from automaton identifiers to automata 
	 *   (of type <code>Automaton</code>).
	 * @exception IllegalArgumentException if this regular expression uses
	 *   a named identifier that does not occur in the automaton map
	 */
	public Automaton toAutomaton(Map<String, Automaton> automata) throws IllegalArgumentException {
		return toAutomatonAllowMutate(automata, null, true);
	}
	
	/** 
	 * Constructs new <code>Automaton</code> from this <code>RegExp</code>. 
	 * The constructed automaton has no transitions to dead states. 
	 * @param automata a map from automaton identifiers to automata 
	 *   (of type <code>Automaton</code>).
	 * @param minimize if set, the automaton is minimized and determinized
	 * @exception IllegalArgumentException if this regular expression uses
	 *   a named identifier that does not occur in the automaton map
	 */
	public Automaton toAutomaton(Map<String, Automaton> automata, boolean minimize) throws IllegalArgumentException {
		return toAutomatonAllowMutate(automata, null, minimize);
	}
	
	/**
	 * Sets or resets allow mutate flag.
	 * If this flag is set, then automata construction uses mutable automata,
	 * which is slightly faster but not thread safe. 
	 * By default, the flag is not set.
	 * @param flag if true, the flag is set
	 * @return previous value of the flag
	 */
	public boolean setAllowMutate(boolean flag) {
		boolean b = allow_mutation;
		allow_mutation = flag;
		return b;
	}
	
	private Automaton toAutomatonAllowMutate(Map<String, Automaton> automata, 
			AutomatonProvider automaton_provider,
			boolean minimize) throws IllegalArgumentException {
		boolean b = false;
		if (allow_mutation)
			b = Automaton.setAllowMutate(true); // thread unsafe
		Automaton a = toAutomaton(automata, automaton_provider, minimize);
		if (allow_mutation)
			Automaton.setAllowMutate(b);
		return a;
	}
		
	private Automaton toAutomaton(Map<String, Automaton> automata, 
			AutomatonProvider automaton_provider,
			boolean minimize) throws IllegalArgumentException {
		List<Automaton> list;
		Automaton a = null;
		switch (_kind) {
		case REGEXP_UNION:
			list = new ArrayList<Automaton>();
			findLeaves(_exp1, Kind.REGEXP_UNION, list, automata, automaton_provider, minimize);
			findLeaves(_exp2, Kind.REGEXP_UNION, list, automata, automaton_provider, minimize);
			a = BasicOperations.union(list);
			if (minimize)
				a.minimize();
			break;
		case REGEXP_CONCATENATION:
			list = new ArrayList<Automaton>();
			findLeaves(_exp1, Kind.REGEXP_CONCATENATION, list, automata, automaton_provider, minimize);
			findLeaves(_exp2, Kind.REGEXP_CONCATENATION, list, automata, automaton_provider, minimize);
			a = BasicOperations.concatenate(list);
			if (minimize)
				a.minimize();
			break;
		case REGEXP_INTERSECTION:
			a = _exp1.toAutomaton(automata, automaton_provider, minimize).intersection(
					_exp2.toAutomaton(automata, automaton_provider, minimize));
			if (minimize)
				a.minimize();
			break;
		case REGEXP_OPTIONAL:
			a = _exp1.toAutomaton(automata, automaton_provider, minimize).optional();
			if (minimize)
				a.minimize();
			break;
		case REGEXP_REPEAT:
			a = _exp1.toAutomaton(automata, automaton_provider, minimize).repeat();
			if (minimize)
				a.minimize();
			break;
		case REGEXP_REPEAT_MIN:
			a = _exp1.toAutomaton(automata, automaton_provider, minimize).repeat(_min);
			if (minimize)
				a.minimize();
			break;
		case REGEXP_REPEAT_MINMAX:
			a = _exp1.toAutomaton(automata, automaton_provider, minimize).repeat(_min, _max);
			if (minimize)
				a.minimize();
			break;
		case REGEXP_COMPLEMENT:
			a = _exp1.toAutomaton(automata, automaton_provider, minimize).complement();
			if (minimize)
				a.minimize();
			break;
		case REGEXP_CHAR:
			a = BasicAutomata.makeChar(_char);
			break;
		case REGEXP_CHAR_RANGE:
			a = BasicAutomata.makeCharRange(_from, _to);
			break;
		case REGEXP_ANYCHAR:
			a = BasicAutomata.makeAnyChar();
			break;
		case REGEXP_EMPTY:
			a = BasicAutomata.makeEmpty();
			break;
		case REGEXP_STRING:
			a = BasicAutomata.makeString(_processedString);
			break;
		case REGEXP_ANYSTRING:
			a = BasicAutomata.makeAnyString();
			break;
		case REGEXP_AUTOMATON:
			Automaton aa = null;
			if (automata != null)
				aa = automata.get(_processedString);
			if (aa == null && automaton_provider != null)
				try {
					aa = automaton_provider.getAutomaton(_processedString);
				} catch (IOException e) {
					throw new IllegalArgumentException(e);
				}
			if (aa == null)
				throw new IllegalArgumentException("'" + _processedString + "' not found");
			a = aa.clone(); // always clone here (ignore allow_mutate)
			break;
		case REGEXP_INTERVAL:
			a = BasicAutomata.makeInterval(_min, _max, _digits);
			break;
		}
		return a;
	}

	private void findLeaves(RegExp exp, Kind kind, List<Automaton> list, Map<String, Automaton> automata, 
			AutomatonProvider automaton_provider,
			boolean minimize) {
		if (exp._kind == kind) {
			findLeaves(exp._exp1, kind, list, automata, automaton_provider, minimize);
			findLeaves(exp._exp2, kind, list, automata, automaton_provider, minimize);
		} else
			list.add(exp.toAutomaton(automata, automaton_provider, minimize));
	}

	/** 
	 * Constructs string from parsed regular expression. 
	 */
	@Override
	public String toString() {
		return toStringBuilder(new StringBuilder()).toString();
	}

	StringBuilder toStringBuilder(StringBuilder b) {
		switch (_kind) {
		case REGEXP_UNION:
			b.append("(");
			_exp1.toStringBuilder(b);
			b.append("|");
			_exp2.toStringBuilder(b);
			b.append(")");
			break;
		case REGEXP_CONCATENATION:
			_exp1.toStringBuilder(b);
			_exp2.toStringBuilder(b);
			break;
		case REGEXP_INTERSECTION:
			b.append("(");
			_exp1.toStringBuilder(b);
			b.append("&");
			_exp2.toStringBuilder(b);
			b.append(")");
			break;
		case REGEXP_OPTIONAL:
			b.append("(");
			_exp1.toStringBuilder(b);
			b.append(")?");
			break;
		case REGEXP_REPEAT:
			b.append("(");
			_exp1.toStringBuilder(b);
			b.append(")*");
			break;
		case REGEXP_REPEAT_MIN:
			b.append("(");
			_exp1.toStringBuilder(b);
			b.append("){").append(_min).append(",}");
			break;
		case REGEXP_REPEAT_MINMAX:
			b.append("(");
			_exp1.toStringBuilder(b);
			b.append("){").append(_min).append(",").append(_max).append("}");
			break;
		case REGEXP_COMPLEMENT:
			b.append("~(");
			_exp1.toStringBuilder(b);
			b.append(")");
			break;
		case REGEXP_CHAR:
			appendChar(_char, b);
			break;
		case REGEXP_CHAR_RANGE:
			b.append("[\\").append(_from).append("-\\").append(_to).append("]");
			break;
		case REGEXP_ANYCHAR:
			b.append(".");
			break;
		case REGEXP_EMPTY:
			b.append("#");
			break;
		case REGEXP_STRING:
			if (_processedString.indexOf('"') == -1) {
				b.append("\"").append(_processedString).append("\"");
			} else {
				for (int i = 0; i < _processedString.length(); i++) {
					appendChar(_processedString.charAt(i), b);
				}
			}
			break;
		case REGEXP_ANYSTRING:
			b.append("@");
			break;
		case REGEXP_AUTOMATON:
			b.append("<").append(_processedString).append(">");
			break;
		case REGEXP_INTERVAL:
			String s1 = Integer.toString(_min);
			String s2 = Integer.toString(_max);
			b.append("<");
			if (_digits > 0)
				for (int i = s1.length(); i < _digits; i++)
					b.append('0');
			b.append(s1).append("-");
			if (_digits > 0)
				for (int i = s2.length(); i < _digits; i++)
					b.append('0');
			b.append(s2).append(">");
			break;
		}
		return b;
	}

	private void appendChar(char c, StringBuilder b) {
		if ("|&?*+{},![]^-.#@\"()<>\\".indexOf(c) != -1) {
			b.append("\\");
		}
		b.append(c);
	}

	/** 
	 * Returns set of automaton identifiers that occur in this regular expression. 
	 */
	public Set<String> getIdentifiers() {
		HashSet<String> set = new HashSet<String>();
		getIdentifiers(set);
		return set;
	}

	void getIdentifiers(Set<String> set) {
		switch (_kind) {
		case REGEXP_UNION:
		case REGEXP_CONCATENATION:
		case REGEXP_INTERSECTION:
			_exp1.getIdentifiers(set);
			_exp2.getIdentifiers(set);
			break;
		case REGEXP_OPTIONAL:
		case REGEXP_REPEAT:
		case REGEXP_REPEAT_MIN:
		case REGEXP_REPEAT_MINMAX:
		case REGEXP_COMPLEMENT:
			_exp1.getIdentifiers(set);
			break;
		case REGEXP_AUTOMATON:
			set.add(_processedString);
			break;
		default:
		}
	}

	static RegExp makeUnion(RegExp exp1, RegExp exp2) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_UNION;
		r._exp1 = exp1;
		r._exp2 = exp2;
		return r;
	}

	static RegExp makeConcatenation(RegExp exp1, RegExp exp2) {
		if ((exp1._kind == Kind.REGEXP_CHAR || exp1._kind == Kind.REGEXP_STRING) &&
			(exp2._kind == Kind.REGEXP_CHAR || exp2._kind == Kind.REGEXP_STRING))
			return makeString(exp1, exp2);
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_CONCATENATION;
		if (exp1._kind == Kind.REGEXP_CONCATENATION &&
			(exp1._exp2._kind == Kind.REGEXP_CHAR || exp1._exp2._kind == Kind.REGEXP_STRING) &&
			(exp2._kind == Kind.REGEXP_CHAR || exp2._kind == Kind.REGEXP_STRING)) {
			r._exp1 = exp1._exp1;
			r._exp2 = makeString(exp1._exp2, exp2);
		} else if ((exp1._kind == Kind.REGEXP_CHAR || exp1._kind == Kind.REGEXP_STRING) &&
				   exp2._kind == Kind.REGEXP_CONCATENATION &&
				   (exp2._exp1._kind == Kind.REGEXP_CHAR || exp2._exp1._kind == Kind.REGEXP_STRING)) {
			r._exp1 = makeString(exp1, exp2._exp1);
			r._exp2 = exp2._exp2;
		} else {
			r._exp1 = exp1;
			r._exp2 = exp2;
		}
		return r;
	}

	static private RegExp makeString(RegExp exp1, RegExp exp2) {
		StringBuilder b = new StringBuilder();
		if (exp1._kind == Kind.REGEXP_STRING)
			b.append(exp1._processedString);
		else
			b.append(exp1._char);
		if (exp2._kind == Kind.REGEXP_STRING)
			b.append(exp2._processedString);
		else
			b.append(exp2._char);
		return makeString(b.toString());
	}

	static RegExp makeIntersection(RegExp exp1, RegExp exp2) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_INTERSECTION;
		r._exp1 = exp1;
		r._exp2 = exp2;
		return r;
	}

	static RegExp makeOptional(RegExp exp) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_OPTIONAL;
		r._exp1 = exp;
		return r;
	}

	static RegExp makeRepeat(RegExp exp) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_REPEAT;
		r._exp1 = exp;
		return r;
	}

	static RegExp makeRepeat(RegExp exp, int min) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_REPEAT_MIN;
		r._exp1 = exp;
		r._min = min;
		return r;
	}

	static RegExp makeRepeat(RegExp exp, int min, int max) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_REPEAT_MINMAX;
		r._exp1 = exp;
		r._min = min;
		r._max = max;
		return r;
	}

	static RegExp makeComplement(RegExp exp) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_COMPLEMENT;
		r._exp1 = exp;
		return r;
	}

	static RegExp makeChar(char c) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_CHAR;
		r._char = c;
		return r;
	}

	static RegExp makeCharRange(char from, char to) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_CHAR_RANGE;
		r._from = from;
		r._to = to;
		return r;
	}

	static RegExp makeAnyChar() {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_ANYCHAR;
		return r;
	}

	static RegExp makeEmpty() {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_EMPTY;
		return r;
	}

	static RegExp makeString(String s) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_STRING;
		r._processedString = s;
		return r;
	}

	static RegExp makeAnyString() {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_ANYSTRING;
		return r;
	}

	static RegExp makeAutomaton(String s) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_AUTOMATON;
		r._processedString = s;
		return r;
	}

	static RegExp makeInterval(int min, int max, int digits) {
		RegExp r = new RegExp();
		r._kind = Kind.REGEXP_INTERVAL;
		r._min = min;
		r._max = max;
		r._digits = digits;
		return r;
	}

	private boolean peek(String s) {
		return more() && s.indexOf(_inputString.charAt(_pos)) != -1;
	}

	private boolean match(char c) {
		if (_pos >= _inputString.length())
			return false;
		if (_inputString.charAt(_pos) == c) {
			_pos++;
			return true;
		}
		return false;
	}

	private boolean more() {
		return _pos < _inputString.length();
	}

	private char next() throws IllegalArgumentException {
		if (!more())
			throw new IllegalArgumentException("unexpected end-of-string");
		return _inputString.charAt(_pos++);
	}

	private boolean check(int flag) {
		return (_flags & flag) != 0;
	}

	final RegExp parseUnionExp() throws IllegalArgumentException {
		RegExp e = parseInterExp();
		if (match('|'))
			e = makeUnion(e, parseUnionExp());
		return e;
	}

	final RegExp parseInterExp() throws IllegalArgumentException {
		RegExp e = parseConcatExp();
		if (check(INTERSECTION) && match('&'))
			e = makeIntersection(e, parseInterExp());
		return e;
	}

	final RegExp parseConcatExp() throws IllegalArgumentException {
		RegExp e = parseRepeatExp();
		if (more() && !peek(")|") && (!check(INTERSECTION) || !peek("&")))
			e = makeConcatenation(e, parseConcatExp());
		return e;
	}

	final RegExp parseRepeatExp() throws IllegalArgumentException {
		RegExp e = parseComplExp();
		while (peek("?*+{")) {
			if (match('?'))
				e = makeOptional(e);
			else if (match('*'))
				e = makeRepeat(e);
			else if (match('+'))
				e = makeRepeat(e, 1);
			else if (match('{')) {
				int start = _pos;
				while (peek("0123456789"))
					next();
				if (start == _pos)
					throw new IllegalArgumentException("integer expected at position " + _pos);
				int n = Integer.parseInt(_inputString.substring(start, _pos));
				int m = -1;
				if (match(',')) {
					start = _pos;
					while (peek("0123456789"))
						next();
					if (start != _pos)
						m = Integer.parseInt(_inputString.substring(start, _pos));
				} else
					m = n;
				if (!match('}'))
					throw new IllegalArgumentException("expected '}' at position " + _pos);
				if (m == -1)
					e = makeRepeat(e, n);
				else
					e = makeRepeat(e, n, m);
			}
		}
		return e;
	}

	final RegExp parseComplExp() throws IllegalArgumentException {
		if (check(COMPLEMENT) && match('~'))
			return makeComplement(parseComplExp());
		else
			return parseCharClassExp();
	}

	final RegExp parseCharClassExp() throws IllegalArgumentException {
		if (match('[')) {
			boolean negate = false;
			if (match('^'))
				negate = true;
			RegExp e = parseCharClasses();
			if (negate)
				e = makeIntersection(makeAnyChar(), makeComplement(e));
			if (!match(']'))
				throw new IllegalArgumentException("expected ']' at position " + _pos);
			return e;
		} else
			return parseSimpleExp();
	}

	final RegExp parseCharClasses() throws IllegalArgumentException {
		RegExp e = parseCharClass();
		while (more() && !peek("]"))
			e = makeUnion(e, parseCharClass());
		return e;
	}

	final RegExp parseCharClass() throws IllegalArgumentException {
		char c = parseCharExp();
		if (match('-'))
			if (peek("]"))
                return makeUnion(makeChar(c), makeChar('-'));
            else
                return makeCharRange(c, parseCharExp());
		else
			return makeChar(c);
	}

	final RegExp parseSimpleExp() throws IllegalArgumentException {
		if (match('.'))
			return makeAnyChar();
		else if (check(EMPTY) && match('#'))
			return makeEmpty();
		else if (check(ANYSTRING) && match('@'))
			return makeAnyString();
		else if (match('"')) {
			int start = _pos;
			while (more() && !peek("\""))
				next();
			if (!match('"'))
				throw new IllegalArgumentException("expected '\"' at position " + _pos);
			return makeString(_inputString.substring(start, _pos - 1));
		} else if (match('(')) {
			if (match(')'))
				return makeString("");
			RegExp e = parseUnionExp();
			if (!match(')'))
				throw new IllegalArgumentException("expected ')' at position " + _pos);
			return e;
		} else if ((check(AUTOMATON) || check(INTERVAL)) && match('<')) {
			int start = _pos;
			while (more() && !peek(">"))
				next();
			if (!match('>'))
				throw new IllegalArgumentException("expected '>' at position " + _pos);
			String s = _inputString.substring(start, _pos - 1);
			int i = s.indexOf('-');
			if (i == -1) {
				if (!check(AUTOMATON))
					throw new IllegalArgumentException("interval syntax error at position " + (_pos - 1));
				return makeAutomaton(s);
			} else {
				if (!check(INTERVAL))
					throw new IllegalArgumentException("illegal identifier at position " + (_pos - 1));
				try {
					if (i == 0 || i == s.length() - 1 || i != s.lastIndexOf('-'))
						throw new NumberFormatException();
					String smin = s.substring(0, i);
					String smax = s.substring(i + 1, s.length());
					int imin = Integer.parseInt(smin);
					int imax = Integer.parseInt(smax);
					int digits;
					if (smin.length() == smax.length())
						digits = smin.length();
					else
						digits = 0;
					if (imin > imax) {
						int t = imin;
						imin = imax;
						imax = t;
					}
					return makeInterval(imin, imax, digits);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("interval syntax error at position " + (_pos - 1));
				}
			}
		} else
			return makeChar(parseCharExp());
	}

	final char parseCharExp() throws IllegalArgumentException {
		match('\\');
		return next();
	}
}
