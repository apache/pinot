package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class MutableFSTTest {
  @Test
  public void shouldCompactNulls1()
      throws Exception {
    List<Integer> listGood = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9);
    List<Integer> listBad = Lists.newArrayList(null, 1, 2, null, 3, 4, null, 5, 6, null, 7, 8, 9, null);
    MutableFST.compactNulls((ArrayList) listBad);
    assertEquals(listGood, listBad);
  }

  @Test
  public void shouldCompactNulls2()
      throws Exception {
    ArrayList<Integer> listBad = (ArrayList) Lists.newArrayList(1);
    MutableFST.compactNulls(listBad);
    assertEquals(Lists.newArrayList(1), listBad);
  }

  @Test
  public void shouldCreateWithStateSymbols()
      throws Exception {
    MutableFST fst = createStateSymbolFst();

    assertEquals(5, fst.getStateCount());
    assertEquals(1, fst.getState(0).getArcCount()); // start
    assertEquals(2, fst.getState(1).getArcCount()); // _B
    assertEquals(1, fst.getState(2).getArcCount()); // _A
    assertEquals(0, fst.getState(3).getArcCount()); // _C
    assertEquals(0, fst.getState(4).getArcCount()); // _D

    assertEquals(4, fst.getInputSymbols().size());
    assertEquals(4, fst.getOutputSymbols().size());

    MutableState stateA = fst.getState(2);
    assertEquals("_A", fst.getStateSymbols().invert().keyForId(stateA.getId()));
    assertEquals(1, stateA.getArcCount());
    MutableArc arc = stateA.getArc(0);
    assertEquals(fst.lookupInputSymbol("b"), arc.getIlabel());
    assertEquals(fst.lookupOutputSymbol("B"), arc.getOlabel());
    assertEquals(fst.getState("_B").getId(), arc.getNextState().getId());
    assertTrue(arc.hashCode() != 0);
    assertTrue(StringUtils.isNotBlank(arc.toString()));
  }

  @Test
  public void shouldCopyWithTranslatedSymbols() {
    MutableFST fst = new MutableFST();
    MutableState s0 = fst.newStartState("<start>");
    MutableState s1 = fst.newState();
    MutableState s2 = fst.newState();
    fst.getInputSymbols().getOrAdd("a");
    fst.getInputSymbols().getOrAdd("b");
    fst.getInputSymbols().getOrAdd("c");
    fst.getInputSymbols().getOrAdd("d");
    fst.getOutputSymbols().getOrAdd("A");
    fst.getOutputSymbols().getOrAdd("B");
    fst.getOutputSymbols().getOrAdd("C");
    fst.getOutputSymbols().getOrAdd("D");
    fst.addArc(s0, "a", "A", s1);
    fst.addArc(s1, "b", "B", s2);
    fst.addArc(s0, "c", "C", s2);
    fst.addArc(s2, "d", "D", s2);

    MutableSymbolTable newIn = new MutableSymbolTable();
    newIn.put("a", 101);
    newIn.put("b", 102);
    newIn.put("c", 103);
    newIn.put("d", 104);
    MutableSymbolTable newOut = new MutableSymbolTable();
    newOut.put("A", 201);
    newOut.put("B", 202);
    newOut.put("C", 203);
    newOut.put("D", 204);

    MutableFST result = MutableFST.copyFrom(fst);

    MutableState rs0 = result.getState(0);
    MutableState rs1 = result.getState(1);
    MutableState rs2 = result.getState(2);
    assertEquals(101, rs0.getArc(0).getIlabel());
    assertEquals(201, rs0.getArc(0).getOlabel());
    assertEquals(103, rs0.getArc(1).getIlabel());
    assertEquals(203, rs0.getArc(1).getOlabel());

    assertEquals(102, rs1.getArc(0).getIlabel());
    assertEquals(202, rs1.getArc(0).getOlabel());

    assertEquals(104, rs2.getArc(0).getIlabel());
    assertEquals(204, rs2.getArc(0).getOlabel());
  }

  private MutableFST createStateSymbolFst() {
    MutableFST fst = new MutableFST();

    MutableState startState = fst.newStartState("<start>");

    // creating a few symbols by hand, others will get created automatically
    fst.newState("_B");
    int inputA = fst.getInputSymbols().getOrAdd("a");

    fst.addArc("<start>", "a", "A", "_A");
    fst.addArc("_A", "b", "B", "_B");
    fst.addArc("_B", "c", "C", "_C");
    fst.addArc("_B", "d", "D", "_D");

    return fst;
  }
}
