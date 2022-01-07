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
  public void shouldCompactNulls1()
      throws Exception {
    List<Integer> listGood = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9);
    List<Integer> listBad = Lists.newArrayList(null, 1, 2, null, 3, 4, null, 5, 6, null, 7, 8, 9, null);
    MutableFSTImpl.compactNulls((ArrayList) listBad);
    assertEquals(listGood, listBad);
  }

  @Test
  public void shouldCompactNulls2()
      throws Exception {
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
    assertEquals("_A", fst.getStateSymbols().invert().keyForId(stateA.getId()));
    assertEquals(1, stateA.getArcCount());
    MutableArc arc = stateA.getArc(0);

    assertEquals(fst.getState("_B").getId(), arc.getNextState().getId());
    assertTrue(arc.hashCode() != 0);
    assertTrue(StringUtils.isNotBlank(arc.toString()));
  }

  private MutableFSTImpl createStateSymbolFst() {
    MutableFSTImpl fst = new MutableFSTImpl();

    fst.newStartState("<start>");

    // creating a few symbols by hand, others will get created automatically
    fst.newState("_B");

    fst.addArc("<start>", "a", "_A");
    fst.addArc("_A", "b", "_B");
    fst.addArc("_B", "c", "_C");
    fst.addArc("_B", "d", "_D");

    return fst;
  }

  private MutableFSTImpl createStateSymbolFst2() {
    MutableFSTImpl fst = new MutableFSTImpl();

    fst.newStartState("<start>");

    // creating a few symbols by hand, others will get created automatically
    fst.newState("_B");

    fst.addArc("<start>", "a", "_A");
    fst.addArc("<start>", "b", "_B");
    fst.addArc("_A", "g", "_C");
    fst.addArc("_B", "a", "_D");
    fst.addArc("_D", "t", "_E");
    fst.addArc("_C", "e", "_F");

    fst.getState("_F").setIsTerminal(true);
    fst.getState("_E").setIsTerminal(true);

    return fst;
  }

  @Test
  public void testTraversalFoo() {
    MutableFSTImpl fst = createStateSymbolFst2();

   /* assertEquals(5, fst.getStateCount());
    assertEquals(1, fst.getState(0).getArcCount()); // start
    assertEquals(2, fst.getState(1).getArcCount()); // _B
    assertEquals(1, fst.getState(2).getArcCount()); // _A
    assertEquals(0, fst.getState(3).getArcCount()); // _C
    assertEquals(0, fst.getState(4).getArcCount()); // _D*/

    List<MutableArc> arcs = fst.getStartState().getArcs();

    //int pos = fst.lookupOutputSymbol("b");

    for (MutableArc arc : arcs) {
      if (arc.getOutputSymbol().matches("b")) {
        MutableState state = arc.getNextState();
        arcs = state.getArcs();

        for (MutableArc arc1 : arcs) {
          if (arc1.getOutputSymbol().matches("a")) {
            state = arc1.getNextState();
            arcs = state.getArcs();

            for (MutableArc arc2 : arcs) {
              if (arc2.getOutputSymbol().matches("t")) {
                state = arc2.getNextState();

                if (state.isTerminal()) {
                  System.out.println("DONE");
                }
              }
            }
          }
        }
      }
    }

    //assertEquals(4, fst.getOutputSymbols().size());



    MutableState stateA = fst.getState(2);
    assertEquals("_A", fst.getStateSymbols().invert().keyForId(stateA.getId()));
    assertEquals(1, stateA.getArcCount());
    MutableArc arc = stateA.getArc(0);

    assertEquals(fst.getState("_B").getId(), arc.getNextState().getId());
    assertTrue(arc.hashCode() != 0);
    assertTrue(StringUtils.isNotBlank(arc.toString()));
  }
}
