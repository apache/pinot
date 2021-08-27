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
package org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.builders;

import com.carrotsearch.hppc.IntIntHashMap;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.BitSet;
import java.util.TreeMap;
import org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.FSA;
import org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.FSA5;
import org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.FSAFlags;
import org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.StateVisitor;


/**
 * Other FSA-related utilities not directly associated with the class hierarchy.
 */
public final class FSAUtils {
  public final static class IntIntHolder {
    public int a;
    public int b;

    public IntIntHolder(int a, int b) {
      this.a = a;
      this.b = b;
    }

    public IntIntHolder() {
    }
  }

  /**
   * Returns the right-language reachable from a given FSA node, formatted as an
   * input for the graphviz package (expressed in the <code>dot</code>
   * language).
   * 
   * @param fsa The automaton to visualize.
   * @param node Starting node (subgraph will be visualized unless it's the automaton's root node).
   * @return Returns the dot language description of the automaton.
   */
  public static String toDot(FSA fsa, int node) {
    try {
      StringWriter w = new StringWriter();
      toDot(w, fsa, node);
      return w.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Saves the right-language reachable from a given FSA node, formatted as an
   * input for the graphviz package (expressed in the <code>dot</code>
   * language), to the given writer.
   * 
   * @param w The writer to write dot language description of the automaton. 
   * @param fsa The automaton to visualize.
   * @param node Starting node (subgraph will be visualized unless it's the automaton's root node).
   * @throws IOException Rethrown if an I/O exception occurs. 
   */
  public static void toDot(Writer w, FSA fsa, int node) throws IOException {
    w.write("digraph Automaton {\n");
    w.write("  rankdir = LR;\n");

    final BitSet visited = new BitSet();

    w.write("  stop [shape=doublecircle,label=\"\"];\n");
    w.write("  initial [shape=plaintext,label=\"\"];\n");
    w.write("  initial -> " + node + "\n\n");

    visitNode(w, 0, fsa, node, visited);
    w.write("}\n");
  }

  private static void visitNode(Writer w, int d, FSA fsa, int s, BitSet visited) throws IOException {
    visited.set(s);
    w.write("  ");
    w.write(Integer.toString(s));

    if (fsa.getFlags().contains(FSAFlags.NUMBERS)) {
      int nodeNumber = fsa.getRightLanguageCount(s);
      w.write(" [shape=circle,label=\"" + nodeNumber + "\"];\n");
    } else {
      w.write(" [shape=circle,label=\"\"];\n");
    }

    for (int arc = fsa.getFirstArc(s); arc != 0; arc = fsa.getNextArc(arc)) {
      w.write("  ");
      w.write(Integer.toString(s));
      w.write(" -> ");
      if (fsa.isArcTerminal(arc)) {
        w.write("stop");
      } else {
        w.write(Integer.toString(fsa.getEndNode(arc)));
      }

      final byte label = fsa.getArcLabel(arc);
      w.write(" [label=\"");
      if (Character.isLetterOrDigit(label))
        w.write((char) label);
      else {
        w.write("0x");
        w.write(Integer.toHexString(label & 0xFF));
      }
      w.write("\"");
      if (fsa.isArcFinal(arc))
        w.write(" arrowhead=\"tee\"");
      if (fsa instanceof FSA5) {
        if (((FSA5) fsa).isNextSet(arc)) {
          w.write(" color=\"blue\"");
        }
      }

      w.write("]\n");
    }

    for (int arc = fsa.getFirstArc(s); arc != 0; arc = fsa.getNextArc(arc)) {
      if (!fsa.isArcTerminal(arc)) {
        int endNode = fsa.getEndNode(arc);
        if (!visited.get(endNode)) {
          visitNode(w, d + 1, fsa, endNode, visited);
        }
      }
    }
  }

  /**
   * Calculate fan-out ratio (how many nodes have a given number of outgoing arcs).  
   * 
   * @param fsa The automaton to calculate fanout for. 
   * @param root The starting node for calculations.
   * 
   * @return The returned map contains keys for the number of outgoing arcs and
   * an associated value being the number of nodes with that arc number. 
   */
  public static TreeMap<Integer, Integer> calculateFanOuts(final FSA fsa, int root) {
    final int[] result = new int[256];
    fsa.visitInPreOrder(new StateVisitor() {
      public boolean accept(int state) {
        int count = 0;
        for (int arc = fsa.getFirstArc(state); arc != 0; arc = fsa.getNextArc(arc)) {
          count++;
        }
        result[count]++;
        return true;
      }
    });

    TreeMap<Integer, Integer> output = new TreeMap<Integer, Integer>();

    int low = 1; // Omit #0, there is always a single node like that (dummy).
    while (low < result.length && result[low] == 0) {
      low++;
    }

    int high = result.length - 1;
    while (high >= 0 && result[high] == 0) {
      high--;
    }

    for (int i = low; i <= high; i++) {
      output.put(i, result[i]);
    }

    return output;
  }

  /**
   * Calculate the size of "right language" for each state in an FSA. The right
   * language is the number of sequences encoded from a given node in the automaton.
   * 
   * @param fsa The automaton to calculate right language for.
   * @return Returns a map with node identifiers as keys and their right language
   * counts as associated values. 
   */
  public static IntIntHashMap rightLanguageForAllStates(final FSA fsa) {
    final IntIntHashMap numbers = new IntIntHashMap();

    fsa.visitInPostOrder(new StateVisitor() {
      public boolean accept(int state) {
        int thisNodeNumber = 0;
        for (int arc = fsa.getFirstArc(state); arc != 0; arc = fsa.getNextArc(arc)) {
          thisNodeNumber += (fsa.isArcFinal(arc) ? 1 : 0)
              + (fsa.isArcTerminal(arc) ? 0 : numbers.get(fsa.getEndNode(arc)));
        }
        numbers.put(state, thisNodeNumber);

        return true;
      }
    });

    return numbers;
  }
}
