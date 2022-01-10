package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class MutableFSTImplTest {
  @Test
  public void shouldCompactNulls1() {
    List<Integer> listGood = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9);
    List<Integer> listBad = Lists.newArrayList(null, 1, 2, null, 3, 4, null, 5, 6, null, 7, 8, 9, null);
    MutableFSTImpl.compactNulls((ArrayList) listBad);
    assertEquals(listGood, listBad);
  }

  @Test
  public void shouldCompactNulls2() {
    ArrayList<Integer> listBad = (ArrayList) Lists.newArrayList(1);
    MutableFSTImpl.compactNulls(listBad);
    assertEquals(Lists.newArrayList(1), listBad);
  }

  @Test
  public void shouldCreateWithStateSymbols() {
    MutableFSTImpl fst = createStateSymbolFst();

    assertEquals(5, fst.getStateCount());
    assertEquals(1, fst.getState(0).getArcCount()); // start
    assertEquals(2, fst.getState(1).getArcCount()); // _B
    assertEquals(1, fst.getState(2).getArcCount()); // _A
    assertEquals(0, fst.getState(3).getArcCount()); // _C
    assertEquals(0, fst.getState(4).getArcCount()); // _D

    assertEquals(4, fst.getOutputSymbols().size());

    MutableState stateA = fst.getState(2);
    assertEquals("_A", fst.getStateSymbols().invert().keyForId(stateA.getLabel()));
    assertEquals(1, stateA.getArcCount());
    MutableArc arc = stateA.getArc(0);

    assertEquals(fst.getState("_B").getLabel(), arc.getNextState().getLabel());
    assertTrue(arc.hashCode() != 0);
    assertTrue(StringUtils.isNotBlank(arc.toString()));
  }

  private MutableFSTImpl createStateSymbolFst() {
    MutableFSTImpl fst = new MutableFSTImpl();

    fst.newStartState("<start>");

    // creating a few symbols by hand, others will get created automatically
    fst.newState("_B");

    fst.addArc("<start>", 1, "_A");
    fst.addArc("_A", 2, "_B");
    fst.addArc("_B", 3, "_C");
    fst.addArc("_B", 4, "_D");

    return fst;
  }

  private MutableFSTImpl createStateSymbolFst2() {
    MutableFSTImpl fst = new MutableFSTImpl();

    addPaths(fst, "age", 1);

    return fst;
  }

  private void addPaths(MutableFST mutableFST, String word, int outputSymbol) {
   MutableState state = (MutableState) mutableFST.getStartState();

    for (int i = 0; i < word.length(); i++) {
      MutableState nextState = new MutableState();

      nextState.setLabel(word.charAt(i));

      int currentOutputSymbol = -1;

      if (i == word.length() - 1) {
        currentOutputSymbol = outputSymbol;
      }

      if (state != null) {
        MutableArc mutableArc = new MutableArc(currentOutputSymbol, nextState);
        state.addArc(mutableArc);
      } else {
        mutableFST.setStartState(nextState);
      }

      state = nextState;
    }

    state.setIsTerminal(true);
  }

  private boolean matchWord(MutableFST mutableFST, String word) {
    return matchWordInternal((MutableState) mutableFST.getStartState(), word, 0);
  }

  private boolean matchWordInternal(MutableState mutableState, String word, int currentPos) {
    if (mutableState.getLabel() == word.charAt(currentPos)) {
      if (mutableState.isTerminal() && currentPos == word.length() - 1) {
        return true;
      }

      List<MutableArc> arcs = mutableState.getArcs();

      for (MutableArc arc : arcs) {
        if (matchWordInternal(arc.getNextState(), word, currentPos + 1)) {
          return true;
        }
      }
    }

    return false;
  }

  @Test
  public void testTraversalFoo() {
    MutableFSTImpl fst = createStateSymbolFst2();

    assertTrue(matchWord(fst, "age"));
    assertTrue(matchWord(fst, "bat") == false);
  }
}
