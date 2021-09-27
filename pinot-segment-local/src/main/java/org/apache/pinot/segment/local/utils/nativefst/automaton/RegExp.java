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
import java.util.List;
import java.util.Map;


/**
 * Regular Expression extension to <code>Automaton</code>.
 */
public class RegExp {

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
  private static boolean _allowMutation = false;
  Kind _kind;
  RegExp _exp1;
  RegExp _exp2;
  String _processedString;
  char _char;
  int _min;
  int _max;
  int _digits;
  char _from;
  char _to;
  String _inputString;
  int _flags;
  int _pos;
  RegExp() {
  }

  /**
   * Constructs new <code>RegExp</code> from a string.
   * Same as <code>RegExp(s, ALL)</code>.
   * @param inputString regexp string
   * @exception IllegalArgumentException if an error occured while parsing the regular expression
   */
  public RegExp(String inputString)
      throws IllegalArgumentException {
    this(inputString, ALL);
  }

  /**
   * Constructs new <code>RegExp</code> from a string.
   * @param inputString regexp string
   * @param syntaxFlags boolean 'or' of optional syntax constructs to be enabled
   * @exception IllegalArgumentException if an error occured while parsing the regular expression
   */
  public RegExp(String inputString, int syntaxFlags)
      throws IllegalArgumentException {
    _inputString = inputString;
    _flags = syntaxFlags;
    RegExp e;
    if (inputString.length() == 0) {
      e = makeString("");
    } else {
      e = parseUnionExp();
      if (_pos < _inputString.length()) {
        throw new IllegalArgumentException("end-of-string expected at position " + _pos);
      }
    }
    _kind = e._kind;
    _exp1 = e._exp1;
    _exp2 = e._exp2;
    _processedString = e._processedString;
    _char = e._char;
    _min = e._min;
    _max = e._max;
    _digits = e._digits;
    _from = e._from;
    _to = e._to;
    _inputString = null;
  }

  static RegExp makeUnion(RegExp exp1, RegExp exp2) {
    RegExp r = new RegExp();
    r._kind = Kind.REGEXP_UNION;
    r._exp1 = exp1;
    r._exp2 = exp2;
    return r;
  }

  static RegExp makeConcatenation(RegExp exp1, RegExp exp2) {
    if ((exp1._kind == Kind.REGEXP_CHAR || exp1._kind == Kind.REGEXP_STRING) && (exp2._kind == Kind.REGEXP_CHAR
        || exp2._kind == Kind.REGEXP_STRING)) {
      return makeString(exp1, exp2);
    }
    RegExp r = new RegExp();
    r._kind = Kind.REGEXP_CONCATENATION;
    if (exp1._kind == Kind.REGEXP_CONCATENATION && (exp1._exp2._kind == Kind.REGEXP_CHAR
        || exp1._exp2._kind == Kind.REGEXP_STRING) && (exp2._kind == Kind.REGEXP_CHAR
        || exp2._kind == Kind.REGEXP_STRING)) {
      r._exp1 = exp1._exp1;
      r._exp2 = makeString(exp1._exp2, exp2);
    } else if ((exp1._kind == Kind.REGEXP_CHAR || exp1._kind == Kind.REGEXP_STRING)
        && exp2._kind == Kind.REGEXP_CONCATENATION && (exp2._exp1._kind == Kind.REGEXP_CHAR
        || exp2._exp1._kind == Kind.REGEXP_STRING)) {
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
    if (exp1._kind == Kind.REGEXP_STRING) {
      b.append(exp1._processedString);
    } else {
      b.append(exp1._char);
    }
    if (exp2._kind == Kind.REGEXP_STRING) {
      b.append(exp2._processedString);
    } else {
      b.append(exp2._char);
    }
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

  /**
   * Constructs new <code>Automaton</code> from this <code>RegExp</code>.
   * Same as <code>toAutomaton(null)</code> (empty automaton map).
   */
  public Automaton toAutomaton() {
    return toAutomatonAllowMutate(null, null, true);
  }

  private Automaton toAutomatonAllowMutate(Map<String, Automaton> automata, AutomatonProvider automatonProvider,
      boolean minimize)
      throws IllegalArgumentException {
    boolean b = false;
    if (_allowMutation) {
      b = Automaton.setAllowMutate(true); // thread unsafe
    }
    Automaton a = toAutomaton(automata, automatonProvider, minimize);
    if (_allowMutation) {
      Automaton.setAllowMutate(b);
    }
    return a;
  }

  private Automaton toAutomaton(Map<String, Automaton> automata, AutomatonProvider automatonProvider, boolean minimize)
      throws IllegalArgumentException {
    List<Automaton> list;
    Automaton a = null;
    switch (_kind) {
      case REGEXP_UNION:
        list = new ArrayList<Automaton>();
        findLeaves(_exp1, Kind.REGEXP_UNION, list, automata, automatonProvider, minimize);
        findLeaves(_exp2, Kind.REGEXP_UNION, list, automata, automatonProvider, minimize);
        a = BasicOperations.union(list);
        if (minimize) {
          a.minimize();
        }
        break;
      case REGEXP_CONCATENATION:
        list = new ArrayList<Automaton>();
        findLeaves(_exp1, Kind.REGEXP_CONCATENATION, list, automata, automatonProvider, minimize);
        findLeaves(_exp2, Kind.REGEXP_CONCATENATION, list, automata, automatonProvider, minimize);
        a = BasicOperations.concatenate(list);
        if (minimize) {
          a.minimize();
        }
        break;
      case REGEXP_INTERSECTION:
        a = _exp1.toAutomaton(automata, automatonProvider, minimize)
            .intersection(_exp2.toAutomaton(automata, automatonProvider, minimize));
        if (minimize) {
          a.minimize();
        }
        break;
      case REGEXP_OPTIONAL:
        a = _exp1.toAutomaton(automata, automatonProvider, minimize).optional();
        if (minimize) {
          a.minimize();
        }
        break;
      case REGEXP_REPEAT:
        a = _exp1.toAutomaton(automata, automatonProvider, minimize).repeat();
        if (minimize) {
          a.minimize();
        }
        break;
      case REGEXP_REPEAT_MIN:
        a = _exp1.toAutomaton(automata, automatonProvider, minimize).repeat(_min);
        if (minimize) {
          a.minimize();
        }
        break;
      case REGEXP_REPEAT_MINMAX:
        a = _exp1.toAutomaton(automata, automatonProvider, minimize).repeat(_min, _max);
        if (minimize) {
          a.minimize();
        }
        break;
      case REGEXP_COMPLEMENT:
        a = _exp1.toAutomaton(automata, automatonProvider, minimize).complement();
        if (minimize) {
          a.minimize();
        }
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
        if (automata != null) {
          aa = automata.get(_processedString);
        }
        if (aa == null && automatonProvider != null) {
          try {
            aa = automatonProvider.getAutomaton(_processedString);
          } catch (IOException e) {
            throw new IllegalArgumentException(e);
          }
        }
        if (aa == null) {
          throw new IllegalArgumentException("'" + _processedString + "' not found");
        }
        a = aa.clone(); // always clone here (ignore allow_mutate)
        break;
      case REGEXP_INTERVAL:
        a = BasicAutomata.makeInterval(_min, _max, _digits);
        break;
      default:
        throw new IllegalStateException("Unknown regexp state");
    }
    return a;
  }

  private void findLeaves(RegExp exp, Kind kind, List<Automaton> list, Map<String, Automaton> automata,
      AutomatonProvider automatonProvider, boolean minimize) {
    if (exp._kind == kind) {
      findLeaves(exp._exp1, kind, list, automata, automatonProvider, minimize);
      findLeaves(exp._exp2, kind, list, automata, automatonProvider, minimize);
    } else {
      list.add(exp.toAutomaton(automata, automatonProvider, minimize));
    }
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
        if (_digits > 0) {
          for (int i = s1.length(); i < _digits; i++) {
            b.append('0');
          }
        }
        b.append(s1).append("-");
        if (_digits > 0) {
          for (int i = s2.length(); i < _digits; i++) {
            b.append('0');
          }
        }
        b.append(s2).append(">");
        break;
      default:
        throw new IllegalStateException("Unknown state");
    }
    return b;
  }

  private void appendChar(char c, StringBuilder b) {
    if ("|&?*+{},![]^-.#@\"()<>\\".indexOf(c) != -1) {
      b.append("\\");
    }
    b.append(c);
  }

  private boolean peek(String s) {
    return more() && s.indexOf(_inputString.charAt(_pos)) != -1;
  }

  private boolean match(char c) {
    if (_pos >= _inputString.length()) {
      return false;
    }
    if (_inputString.charAt(_pos) == c) {
      _pos++;
      return true;
    }
    return false;
  }

  private boolean more() {
    return _pos < _inputString.length();
  }

  private char next()
      throws IllegalArgumentException {
    if (!more()) {
      throw new IllegalArgumentException("unexpected end-of-string");
    }
    return _inputString.charAt(_pos++);
  }

  private boolean check(int flag) {
    return (_flags & flag) != 0;
  }

  final RegExp parseUnionExp()
      throws IllegalArgumentException {
    RegExp e = parseInterExp();
    if (match('|')) {
      e = makeUnion(e, parseUnionExp());
    }
    return e;
  }

  final RegExp parseInterExp()
      throws IllegalArgumentException {
    RegExp e = parseConcatExp();
    if (check(INTERSECTION) && match('&')) {
      e = makeIntersection(e, parseInterExp());
    }
    return e;
  }

  final RegExp parseConcatExp()
      throws IllegalArgumentException {
    RegExp e = parseRepeatExp();
    if (more() && !peek(")|") && (!check(INTERSECTION) || !peek("&"))) {
      e = makeConcatenation(e, parseConcatExp());
    }
    return e;
  }

  final RegExp parseRepeatExp()
      throws IllegalArgumentException {
    RegExp e = parseComplExp();
    while (peek("?*+{")) {
      if (match('?')) {
        e = makeOptional(e);
      } else if (match('*')) {
        e = makeRepeat(e);
      } else if (match('+')) {
        e = makeRepeat(e, 1);
      } else if (match('{')) {
        int start = _pos;
        while (peek("0123456789")) {
          next();
        }
        if (start == _pos) {
          throw new IllegalArgumentException("integer expected at position " + _pos);
        }
        int n = Integer.parseInt(_inputString.substring(start, _pos));
        int m = -1;
        if (match(',')) {
          start = _pos;
          while (peek("0123456789")) {
            next();
          }
          if (start != _pos) {
            m = Integer.parseInt(_inputString.substring(start, _pos));
          }
        } else {
          m = n;
        }
        if (!match('}')) {
          throw new IllegalArgumentException("expected '}' at position " + _pos);
        }
        if (m == -1) {
          e = makeRepeat(e, n);
        } else {
          e = makeRepeat(e, n, m);
        }
      }
    }
    return e;
  }

  final RegExp parseComplExp()
      throws IllegalArgumentException {
    if (check(COMPLEMENT) && match('~')) {
      return makeComplement(parseComplExp());
    } else {
      return parseCharClassExp();
    }
  }

  final RegExp parseCharClassExp()
      throws IllegalArgumentException {
    if (match('[')) {
      boolean negate = false;
      if (match('^')) {
        negate = true;
      }
      RegExp e = parseCharClasses();
      if (negate) {
        e = makeIntersection(makeAnyChar(), makeComplement(e));
      }
      if (!match(']')) {
        throw new IllegalArgumentException("expected ']' at position " + _pos);
      }
      return e;
    } else {
      return parseSimpleExp();
    }
  }

  final RegExp parseCharClasses()
      throws IllegalArgumentException {
    RegExp e = parseCharClass();
    while (more() && !peek("]")) {
      e = makeUnion(e, parseCharClass());
    }
    return e;
  }

  final RegExp parseCharClass()
      throws IllegalArgumentException {
    char c = parseCharExp();
    if (match('-')) {
      if (peek("]")) {
        return makeUnion(makeChar(c), makeChar('-'));
      } else {
        return makeCharRange(c, parseCharExp());
      }
    } else {
      return makeChar(c);
    }
  }

  final RegExp parseSimpleExp()
      throws IllegalArgumentException {
    if (match('.')) {
      return makeAnyChar();
    } else if (check(EMPTY) && match('#')) {
      return makeEmpty();
    } else if (check(ANYSTRING) && match('@')) {
      return makeAnyString();
    } else if (match('"')) {
      int start = _pos;
      while (more() && !peek("\"")) {
        next();
      }
      if (!match('"')) {
        throw new IllegalArgumentException("expected '\"' at position " + _pos);
      }
      return makeString(_inputString.substring(start, _pos - 1));
    } else if (match('(')) {
      if (match(')')) {
        return makeString("");
      }
      RegExp e = parseUnionExp();
      if (!match(')')) {
        throw new IllegalArgumentException("expected ')' at position " + _pos);
      }
      return e;
    } else if ((check(AUTOMATON) || check(INTERVAL)) && match('<')) {
      int start = _pos;
      while (more() && !peek(">")) {
        next();
      }
      if (!match('>')) {
        throw new IllegalArgumentException("expected '>' at position " + _pos);
      }
      String s = _inputString.substring(start, _pos - 1);
      int i = s.indexOf('-');
      if (i == -1) {
        if (!check(AUTOMATON)) {
          throw new IllegalArgumentException("interval syntax error at position " + (_pos - 1));
        }
        return makeAutomaton(s);
      } else {
        if (!check(INTERVAL)) {
          throw new IllegalArgumentException("illegal identifier at position " + (_pos - 1));
        }
        try {
          if (i == 0 || i == s.length() - 1 || i != s.lastIndexOf('-')) {
            throw new NumberFormatException();
          }
          String smin = s.substring(0, i);
          String smax = s.substring(i + 1);
          int imin = Integer.parseInt(smin);
          int imax = Integer.parseInt(smax);
          int digits;
          if (smin.length() == smax.length()) {
            digits = smin.length();
          } else {
            digits = 0;
          }
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
    } else {
      return makeChar(parseCharExp());
    }
  }

  final char parseCharExp()
      throws IllegalArgumentException {
    match('\\');
    return next();
  }

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
}
