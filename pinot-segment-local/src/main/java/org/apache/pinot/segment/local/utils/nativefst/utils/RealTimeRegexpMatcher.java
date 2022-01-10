package org.apache.pinot.segment.local.utils.nativefst.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.pinot.segment.local.utils.nativefst.FST;
import org.apache.pinot.segment.local.utils.nativefst.automaton.Automaton;
import org.apache.pinot.segment.local.utils.nativefst.automaton.CharacterRunAutomaton;
import org.apache.pinot.segment.local.utils.nativefst.automaton.RegExp;
import org.apache.pinot.segment.local.utils.nativefst.automaton.State;
import org.apache.pinot.segment.local.utils.nativefst.automaton.Transition;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableArc;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFST;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableState;
import org.roaringbitmap.IntConsumer;

public class RealTimeRegexpMatcher {
  private final String _regexQuery;
  private final MutableFST _fst;
  private final Automaton _automaton;
  private final IntConsumer _dest;

  public RealTimeRegexpMatcher(String regexQuery, MutableFST fst, IntConsumer dest) {
    _regexQuery = regexQuery;
    _fst = fst;
    _dest = dest;

    _automaton = new RegExp(_regexQuery).toAutomaton();
  }

  public static void regexMatch(String regexQuery, FST fst, IntConsumer dest) {
    RegexpMatcher matcher = new RegexpMatcher(regexQuery, fst, dest);
    matcher.regexMatchOnFST();
  }

  // Matches "input" string with _regexQuery Automaton.
  public boolean match(String input) {
    CharacterRunAutomaton characterRunAutomaton = new CharacterRunAutomaton(_automaton);
    return characterRunAutomaton.run(input);
  }

  /**
   * This function runs matching on automaton built from regexQuery and the FST.
   * FST stores key (string) to a value (Long). Both are state machines and state transition is based on
   * a input character.
   *
   * This algorithm starts with Queue containing (Automaton Start Node, FST Start Node).
   * Each step an entry is popped from the queue:
   *    1) if the automaton state is accept and the FST Node is final (i.e. end node) then the value stored for that FST
   *       is added to the set of result.
   *    2) Else next set of transitions on automaton are gathered and for each transition target node for that character
   *       is figured out in FST Node, resulting pair of (automaton state, fst node) are added to the queue.
   *    3) This process is bound to complete since we are making progression on the FST (which is a DAG) towards final
   *       nodes.
   */
  public void regexMatchOnFST() {
    final List<Path> queue = new ArrayList<>();

    if (_automaton.getNumberOfStates() == 0) {
      return;
    }

    // Automaton start state and FST start node is added to the queue.
    queue.add(new Path(_automaton.getInitialState(), (MutableState) _fst.getStartState(), null, new ArrayList<>()));

    Set<State> acceptStates = _automaton.getAcceptStates();
    while (queue.size() != 0) {
      final Path path = queue.remove(queue.size() - 1);

      // If automaton is in accept state and the fstNode is final (i.e. end node) then add the entry to endNodes which
      // contains the result set.
      if (acceptStates.contains(path._state)) {
        if (path._node.isTerminal()) {
          //endNodes.add((long) _fst.getOutputSymbol(path._fstArc));
          _dest.accept(path._fstArc.getOutputSymbol());
        }
      }

      Set<Transition> stateTransitions = path._state.getTransitionSet();
      Iterator<Transition> iterator = stateTransitions.iterator();

      while (iterator.hasNext()) {
        Transition t = iterator.next();

        final int min = t._min;
        final int max = t._max;

        if (min == max) {
          MutableArc arc = getArcForLabel(path._node, t._min);

          if (arc != null) {
            queue.add(new Path(t._to, arc.getNextState(), arc, path._pathState));
          }
        } else {
          if (path._node != null && path._node.isTerminal()) {
            continue;
          }

          MutableState node;
          MutableArc arc;

          if (path._fstArc == null) {
            // First (dummy) arc, get the actual arc
            arc = path._node.getArc(0);
          } else {
            node = path._fstArc.getNextState();
            arc = node.getArc(0);
          }

          while (arc != 0) {
            byte label = _fst.getArcLabel(arc);

            if (label >= min && label <= max) {
              queue.add(new Path(t._to, _fst.getEndNode(arc), arc, path._pathState));
            }

            arc = _fst.isArcLast(arc) ? 0 : _fst.getNextArc(arc);
          }
        }
      }
    }
  }

  private MutableArc getArcForLabel(MutableState mutableState, char label) {
    List<MutableArc> arcs = mutableState.getArcs();

    for (MutableArc arc : arcs) {
      if (arc.getNextState().getLabel() == label) {
        return arc;
      }
    }

    return null;
  }

  /**
   * Represents a path in the FST traversal directed by the automaton
   */
  public final class Path {
    public final State _state;
    public final MutableState _node;
    public final MutableArc _fstArc;
    // Used for capturing the path taken till the point (for debugging)
    public List<Character> _pathState;

    public Path(State state, MutableState node, MutableArc fstArc, List<Character> pathState) {
      _state = state;
      _node = node;
      _fstArc = fstArc;

      _pathState = pathState;

      _pathState.add((char) _fst.getArcLabel(fstArc));
    }
  }
}