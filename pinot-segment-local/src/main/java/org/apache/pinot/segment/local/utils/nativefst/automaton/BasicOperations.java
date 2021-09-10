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
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Basic automata operations.
 */
public final class BasicOperations {

  private BasicOperations() {
  }

  /**
   * Returns an automaton that accepts the concatenation of the languages of
   * the given automata.
   * <p>
   * Complexity: linear in number of states.
   */
  static public Automaton concatenate(Automaton a1, Automaton a2) {
    if (a1.isSingleton() && a2.isSingleton()) {
      return BasicAutomata.makeString(a1._singleton + a2._singleton);
    }
    if (isEmpty(a1) || isEmpty(a2)) {
      return BasicAutomata.makeEmpty();
    }
    boolean deterministic = a1.isSingleton() && a2.isDeterministic();
    if (a1 == a2) {
      a1 = a1.cloneExpanded();
      a2 = a2.cloneExpanded();
    } else {
      a1 = a1.cloneExpandedIfRequired();
      a2 = a2.cloneExpandedIfRequired();
    }
    for (State s : a1.getAcceptStates()) {
      s._accept = false;
      s.addEpsilon(a2._initial);
    }
    a1._deterministic = deterministic;
    a1.clearHashCode();
    a1.checkMinimizeAlways();
    return a1;
  }

  /**
   * Returns an automaton that accepts the concatenation of the languages of
   * the given automata.
   * <p>
   * Complexity: linear in total number of states.
   */
  static public Automaton concatenate(List<Automaton> l) {
    if (l.isEmpty()) {
      return BasicAutomata.makeEmptyString();
    }
    boolean all_singleton = true;
    for (Automaton a : l) {
      if (!a.isSingleton()) {
        all_singleton = false;
        break;
      }
    }
    if (all_singleton) {
      StringBuilder b = new StringBuilder();
      for (Automaton a : l) {
        b.append(a._singleton);
      }
      return BasicAutomata.makeString(b.toString());
    } else {
      for (Automaton a : l) {
        if (a.isEmpty()) {
          return BasicAutomata.makeEmpty();
        }
      }
      Set<Integer> ids = new HashSet<Integer>();
      for (Automaton a : l) {
        ids.add(System.identityHashCode(a));
      }
      boolean has_aliases = ids.size() != l.size();
      Automaton b = l.get(0);
      if (has_aliases) {
        b = b.cloneExpanded();
      } else {
        b = b.cloneExpandedIfRequired();
      }
      Set<State> ac = b.getAcceptStates();
      boolean first = true;
      for (Automaton a : l) {
        if (first) {
          first = false;
        } else {
          if (a.isEmptyString()) {
            continue;
          }
          Automaton aa = a;
          if (has_aliases) {
            aa = aa.cloneExpanded();
          } else {
            aa = aa.cloneExpandedIfRequired();
          }
          Set<State> ns = aa.getAcceptStates();
          for (State s : ac) {
            s._accept = false;
            s.addEpsilon(aa._initial);
            if (s._accept) {
              ns.add(s);
            }
          }
          ac = ns;
        }
      }
      b._deterministic = false;
      b.clearHashCode();
      b.checkMinimizeAlways();
      return b;
    }
  }

  /**
   * Returns an automaton that accepts the union of the empty string and the
   * language of the given automaton.
   * <p>
   * Complexity: linear in number of states.
   */
  static public Automaton optional(Automaton a) {
    a = a.cloneExpandedIfRequired();
    State s = new State();
    s.addEpsilon(a._initial);
    s._accept = true;
    a._initial = s;
    a._deterministic = false;
    a.clearHashCode();
    a.checkMinimizeAlways();
    return a;
  }

  /**
   * Returns an automaton that accepts the Kleene star (zero or more
   * concatenated repetitions) of the language of the given automaton.
   * Never modifies the input automaton language.
   * <p>
   * Complexity: linear in number of states.
   */
  static public Automaton repeat(Automaton a) {
    a = a.cloneExpanded();
    State s = new State();
    s._accept = true;
    s.addEpsilon(a._initial);
    for (State p : a.getAcceptStates()) {
      p.addEpsilon(s);
    }
    a._initial = s;
    a._deterministic = false;
    a.clearHashCode();
    a.checkMinimizeAlways();
    return a;
  }

  /**
   * Returns an automaton that accepts <code>min</code> or more
   * concatenated repetitions of the language of the given automaton.
   * <p>
   * Complexity: linear in number of states and in <code>min</code>.
   */
  static public Automaton repeat(Automaton a, int min) {
    if (min == 0) {
      return repeat(a);
    }
    List<Automaton> as = new ArrayList<Automaton>();
    while (min-- > 0) {
      as.add(a);
    }
    as.add(repeat(a));
    return concatenate(as);
  }

  /**
   * Returns an automaton that accepts between <code>min</code> and
   * <code>max</code> (including both) concatenated repetitions of the
   * language of the given automaton.
   * <p>
   * Complexity: linear in number of states and in <code>min</code> and
   * <code>max</code>.
   */
  static public Automaton repeat(Automaton a, int min, int max) {
    if (min > max) {
      return BasicAutomata.makeEmpty();
    }
    max -= min;
    a.expandSingleton();
    Automaton b;
    if (min == 0) {
      b = BasicAutomata.makeEmptyString();
    } else if (min == 1) {
      b = a.clone();
    } else {
      List<Automaton> as = new ArrayList<Automaton>();
      while (min-- > 0) {
        as.add(a);
      }
      b = concatenate(as);
    }
    if (max > 0) {
      Automaton d = a.clone();
      while (--max > 0) {
        Automaton c = a.clone();
        for (State p : c.getAcceptStates()) {
          p.addEpsilon(d._initial);
        }
        d = c;
      }
      for (State p : b.getAcceptStates()) {
        p.addEpsilon(d._initial);
      }
      b._deterministic = false;
      b.clearHashCode();
      b.checkMinimizeAlways();
    }
    return b;
  }

  /**
   * Returns a (deterministic) automaton that accepts the complement of the
   * language of the given automaton.
   * <p>
   * Complexity: linear in number of states (if already deterministic).
   */
  static public Automaton complement(Automaton a) {
    a = a.cloneExpandedIfRequired();
    a.determinize();
    a.totalize();
    for (State p : a.getStates()) {
      p._accept = !p._accept;
    }
    a.removeDeadTransitions();
    return a;
  }

  /**
   * Returns a (deterministic) automaton that accepts the intersection of
   * the language of <code>a1</code> and the complement of the language of
   * <code>a2</code>. As a side-effect, the automata may be determinized, if not
   * already deterministic.
   * <p>
   * Complexity: quadratic in number of states (if already deterministic).
   */
  static public Automaton minus(Automaton a1, Automaton a2) {
    if (a1.isEmpty() || a1 == a2) {
      return BasicAutomata.makeEmpty();
    }
    if (a2.isEmpty()) {
      return a1.cloneIfRequired();
    }
    if (a1.isSingleton()) {
      if (a2.run(a1._singleton)) {
        return BasicAutomata.makeEmpty();
      } else {
        return a1.cloneIfRequired();
      }
    }
    return intersection(a1, a2.complement());
  }

  /**
   * Returns an automaton that accepts the intersection of
   * the languages of the given automata.
   * Never modifies the input automata languages.
   * <p>
   * Complexity: quadratic in number of states.
   */
  static public Automaton intersection(Automaton a1, Automaton a2) {
    if (a1.isSingleton()) {
      if (a2.run(a1._singleton)) {
        return a1.cloneIfRequired();
      } else {
        return BasicAutomata.makeEmpty();
      }
    }
    if (a2.isSingleton()) {
      if (a1.run(a2._singleton)) {
        return a2.cloneIfRequired();
      } else {
        return BasicAutomata.makeEmpty();
      }
    }
    if (a1 == a2) {
      return a1.cloneIfRequired();
    }
    Transition[][] transitions1 = Automaton.getSortedTransitions(a1.getStates());
    Transition[][] transitions2 = Automaton.getSortedTransitions(a2.getStates());
    Automaton c = new Automaton();
    LinkedList<StatePair> worklist = new LinkedList<StatePair>();
    HashMap<StatePair, StatePair> newstates = new HashMap<StatePair, StatePair>();
    StatePair p = new StatePair(c._initial, a1._initial, a2._initial);
    worklist.add(p);
    newstates.put(p, p);
    while (worklist.size() > 0) {
      p = worklist.removeFirst();
      p._parentState._accept = p._firstState._accept && p._secondState._accept;
      Transition[] t1 = transitions1[p._firstState._number];
      Transition[] t2 = transitions2[p._secondState._number];
      for (int n1 = 0, b2 = 0; n1 < t1.length; n1++) {
        while (b2 < t2.length && t2[b2]._max < t1[n1]._min) {
          b2++;
        }
        for (int n2 = b2; n2 < t2.length && t1[n1]._max >= t2[n2]._min; n2++) {
          if (t2[n2]._max >= t1[n1]._min) {
            StatePair q = new StatePair(t1[n1]._to, t2[n2]._to);
            StatePair r = newstates.get(q);
            if (r == null) {
              q._parentState = new State();
              worklist.add(q);
              newstates.put(q, q);
              r = q;
            }
            char min = t1[n1]._min > t2[n2]._min ? t1[n1]._min : t2[n2]._min;
            char max = t1[n1]._max < t2[n2]._max ? t1[n1]._max : t2[n2]._max;
            p._parentState._transitionSet.add(new Transition(min, max, r._parentState));
          }
        }
      }
    }
    c._deterministic = a1._deterministic && a2._deterministic;
    c.removeDeadTransitions();
    c.checkMinimizeAlways();
    return c;
  }

  /**
   * Returns true if the language of <code>a1</code> is a subset of the
   * language of <code>a2</code>.
   * As a side-effect, <code>a2</code> is determinized if not already marked as
   * deterministic.
   * <p>
   * Complexity: quadratic in number of states.
   */
  public static boolean subsetOf(Automaton a1, Automaton a2) {
    if (a1 == a2) {
      return true;
    }
    if (a1.isSingleton()) {
      if (a2.isSingleton()) {
        return a1._singleton.equals(a2._singleton);
      }
      return a2.run(a1._singleton);
    }
    a2.determinize();
    Transition[][] transitions1 = Automaton.getSortedTransitions(a1.getStates());
    Transition[][] transitions2 = Automaton.getSortedTransitions(a2.getStates());
    LinkedList<StatePair> worklist = new LinkedList<StatePair>();
    HashSet<StatePair> visited = new HashSet<StatePair>();
    StatePair p = new StatePair(a1._initial, a2._initial);
    worklist.add(p);
    visited.add(p);
    while (worklist.size() > 0) {
      p = worklist.removeFirst();
      if (p._firstState._accept && !p._secondState._accept) {
        return false;
      }
      Transition[] t1 = transitions1[p._firstState._number];
      Transition[] t2 = transitions2[p._secondState._number];
      for (int n1 = 0, b2 = 0; n1 < t1.length; n1++) {
        while (b2 < t2.length && t2[b2]._max < t1[n1]._min) {
          b2++;
        }
        int min1 = t1[n1]._min, max1 = t1[n1]._max;
        for (int n2 = b2; n2 < t2.length && t1[n1]._max >= t2[n2]._min; n2++) {
          if (t2[n2]._min > min1) {
            return false;
          }
          if (t2[n2]._max < Character.MAX_VALUE) {
            min1 = t2[n2]._max + 1;
          } else {
            min1 = Character.MAX_VALUE;
            max1 = Character.MIN_VALUE;
          }
          StatePair q = new StatePair(t1[n1]._to, t2[n2]._to);
          if (!visited.contains(q)) {
            worklist.add(q);
            visited.add(q);
          }
        }
        if (min1 <= max1) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns an automaton that accepts the union of the languages of the given automata.
   * <p>
   * Complexity: linear in number of states.
   */
  public static Automaton union(Automaton a1, Automaton a2) {
    if ((a1.isSingleton() && a2.isSingleton() && a1._singleton.equals(a2._singleton)) || a1 == a2) {
      return a1.cloneIfRequired();
    }
    a1 = a1.cloneExpandedIfRequired();
    a2 = a2.cloneExpandedIfRequired();
    State s = new State();
    s.addEpsilon(a1._initial);
    s.addEpsilon(a2._initial);
    a1._initial = s;
    a1._deterministic = false;
    a1.clearHashCode();
    a1.checkMinimizeAlways();
    return a1;
  }

  /**
   * Returns an automaton that accepts the union of the languages of the given automata.
   * <p>
   * Complexity: linear in number of states.
   */
  public static Automaton union(Collection<Automaton> l) {
    Set<Integer> ids = new HashSet<Integer>();
    for (Automaton a : l) {
      ids.add(System.identityHashCode(a));
    }
    boolean has_aliases = ids.size() != l.size();
    State s = new State();
    for (Automaton b : l) {
      if (b.isEmpty()) {
        continue;
      }
      Automaton bb = b;
      if (has_aliases) {
        bb = bb.cloneExpanded();
      } else {
        bb = bb.cloneExpandedIfRequired();
      }
      s.addEpsilon(bb._initial);
    }
    Automaton a = new Automaton();
    a._initial = s;
    a._deterministic = false;
    a.clearHashCode();
    a.checkMinimizeAlways();
    return a;
  }

  /**
   * Determinizes the given automaton.
   * <p>
   * Complexity: exponential in number of states.
   */
  public static void determinize(Automaton a) {
    if (a._deterministic || a.isSingleton()) {
      return;
    }
    Set<State> initialset = new HashSet<State>();
    initialset.add(a._initial);
    determinize(a, initialset);
  }

  /**
   * Determinizes the given automaton using the given set of initial states.
   */
  static void determinize(Automaton a, Set<State> initialset) {
    char[] points = a.getStartPoints();
    // subset construction
    LinkedList<Set<State>> worklist = new LinkedList<Set<State>>();
    Map<Set<State>, State> newstate = new HashMap<Set<State>, State>();
    worklist.add(initialset);
    a._initial = new State();
    newstate.put(initialset, a._initial);
    while (worklist.size() > 0) {
      Set<State> s = worklist.removeFirst();
      State r = newstate.get(s);
      for (State q : s) {
        if (q._accept) {
          r._accept = true;
          break;
        }
      }
      for (int n = 0; n < points.length; n++) {
        Set<State> p = new HashSet<State>();
        for (State q : s) {
          for (Transition t : q._transitionSet) {
            if (t._min <= points[n] && points[n] <= t._max) {
              p.add(t._to);
            }
          }
        }
        if (!p.isEmpty()) {
          State q = newstate.get(p);
          if (q == null) {
            worklist.add(p);
            q = new State();
            newstate.put(p, q);
          }
          char min = points[n];
          char max;
          if (n + 1 < points.length) {
            max = (char) (points[n + 1] - 1);
          } else {
            max = Character.MAX_VALUE;
          }
          r._transitionSet.add(new Transition(min, max, q));
        }
      }
    }
    a._deterministic = true;
    a.removeDeadTransitions();
  }

  /**
   * Adds epsilon transitions to the given automaton.
   * This method adds extra character interval transitions that are equivalent to the given
   * set of epsilon transitions.
   * @param pairs collection of {@link StatePair} objects representing pairs of source/destination states
   *        where epsilon transitions should be added
   */
  public static void addEpsilons(Automaton a, Collection<StatePair> pairs) {
    a.expandSingleton();
    HashMap<State, HashSet<State>> forward = new HashMap<State, HashSet<State>>();
    HashMap<State, HashSet<State>> back = new HashMap<State, HashSet<State>>();
    for (StatePair p : pairs) {
      HashSet<State> to = forward.get(p._firstState);
      if (to == null) {
        to = new HashSet<State>();
        forward.put(p._firstState, to);
      }
      to.add(p._secondState);
      HashSet<State> from = back.get(p._secondState);
      if (from == null) {
        from = new HashSet<State>();
        back.put(p._secondState, from);
      }
      from.add(p._firstState);
    }
    // calculate epsilon closure
    LinkedList<StatePair> worklist = new LinkedList<StatePair>(pairs);
    HashSet<StatePair> workset = new HashSet<StatePair>(pairs);
    while (!worklist.isEmpty()) {
      StatePair p = worklist.removeFirst();
      workset.remove(p);
      HashSet<State> to = forward.get(p._secondState);
      HashSet<State> from = back.get(p._firstState);
      if (to != null) {
        for (State s : to) {
          StatePair pp = new StatePair(p._firstState, s);
          if (!pairs.contains(pp)) {
            pairs.add(pp);
            forward.get(p._firstState).add(s);
            back.get(s).add(p._firstState);
            worklist.add(pp);
            workset.add(pp);
            if (from != null) {
              for (State q : from) {
                StatePair qq = new StatePair(q, p._firstState);
                if (!workset.contains(qq)) {
                  worklist.add(qq);
                  workset.add(qq);
                }
              }
            }
          }
        }
      }
    }
    // add transitions
    for (StatePair p : pairs) {
      p._firstState.addEpsilon(p._secondState);
    }
    a._deterministic = false;
    a.clearHashCode();
    a.checkMinimizeAlways();
  }

  /**
   * Returns true if the given automaton accepts the empty string and nothing else.
   */
  public static boolean isEmptyString(Automaton a) {
    if (a.isSingleton()) {
      return a._singleton.length() == 0;
    } else {
      return a._initial._accept && a._initial._transitionSet.isEmpty();
    }
  }

  /**
   * Returns true if the given automaton accepts no strings.
   */
  public static boolean isEmpty(Automaton a) {
    if (a.isSingleton()) {
      return false;
    }
    return !a._initial._accept && a._initial._transitionSet.isEmpty();
  }

  /**
   * Returns true if the given automaton accepts all strings.
   */
  public static boolean isTotal(Automaton a) {
    if (a.isSingleton()) {
      return false;
    }
    if (a._initial._accept && a._initial._transitionSet.size() == 1) {
      Transition t = a._initial._transitionSet.iterator().next();
      return t._to == a._initial && t._min == Character.MIN_VALUE && t._max == Character.MAX_VALUE;
    }
    return false;
  }

  /**
   * Returns a shortest accepted/rejected string.
   * If more than one shortest string is found, the lexicographically first of the shortest strings is returned.
   * @param accepted if true, look for accepted strings; otherwise, look for rejected strings
   * @return the string, null if none found
   */
  public static String getShortestExample(Automaton a, boolean accepted) {
    if (a.isSingleton()) {
      if (accepted) {
        return a._singleton;
      } else if (a._singleton.length() > 0) {
        return "";
      } else {
        return "\u0000";
      }
    }
    return getShortestExample(a.getInitialState(), accepted);
  }

  static String getShortestExample(State s, boolean accepted) {
    Map<State, String> path = new HashMap<State, String>();
    LinkedList<State> queue = new LinkedList<State>();
    path.put(s, "");
    queue.add(s);
    String best = null;
    while (!queue.isEmpty()) {
      State q = queue.removeFirst();
      String p = path.get(q);
      if (q._accept == accepted) {
        if (best == null || p.length() < best.length() || (p.length() == best.length() && p.compareTo(best) < 0)) {
          best = p;
        }
      } else {
        for (Transition t : q.getTransitionSet()) {
          String tp = path.get(t._to);
          String np = p + t._min;
          if (tp == null || (tp.length() == np.length() && np.compareTo(tp) < 0)) {
            if (tp == null) {
              queue.addLast(t._to);
            }
            path.put(t._to, np);
          }
        }
      }
    }
    return best;
  }

  /**
   * Returns true if the given string is accepted by the automaton.
   * <p>
   * Complexity: linear in the length of the string.
   * <p>
   * <b>Note:</b> for full performance, use the {@link RunAutomaton} class.
   */
  public static boolean run(Automaton a, String s) {
    if (a.isSingleton()) {
      return s.equals(a._singleton);
    }
    if (a._deterministic) {
      State p = a._initial;
      for (int i = 0; i < s.length(); i++) {
        State q = p.step(s.charAt(i));
        if (q == null) {
          return false;
        }
        p = q;
      }
      return p._accept;
    } else {
      Set<State> states = a.getStates();
      Automaton.setStateNumbers(states);
      LinkedList<State> pp = new LinkedList<State>();
      LinkedList<State> pp_other = new LinkedList<State>();
      BitSet bb = new BitSet(states.size());
      BitSet bb_other = new BitSet(states.size());
      pp.add(a._initial);
      ArrayList<State> dest = new ArrayList<State>();
      boolean accept = a._initial._accept;
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        accept = false;
        pp_other.clear();
        bb_other.clear();
        for (State p : pp) {
          dest.clear();
          p.step(c, dest);
          for (State q : dest) {
            if (q._accept) {
              accept = true;
            }
            if (!bb_other.get(q._number)) {
              bb_other.set(q._number);
              pp_other.add(q);
            }
          }
        }
        LinkedList<State> tp = pp;
        pp = pp_other;
        pp_other = tp;
        BitSet tb = bb;
        bb = bb_other;
        bb_other = tb;
      }
      return accept;
    }
  }
}
