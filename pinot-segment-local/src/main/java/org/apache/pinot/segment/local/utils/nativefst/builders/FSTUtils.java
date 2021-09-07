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
package org.apache.pinot.segment.local.utils.nativefst.builders;

import com.carrotsearch.hppc.IntIntHashMap;
import java.io.IOException;
import java.io.Writer;
import java.util.BitSet;
import java.util.TreeMap;
import org.apache.pinot.segment.local.utils.nativefst.FST;
import org.apache.pinot.segment.local.utils.nativefst.FSTFlags;
import org.apache.pinot.segment.local.utils.nativefst.ImmutableFST;
import org.apache.pinot.segment.local.utils.nativefst.StateVisitor;


/**
 * Other FST-related utilities not directly associated with the class hierarchy.
 */
public final class FSTUtils {

  /**
   * Saves the right-language reachable from a given FST node, formatted as an
   * input for the graphviz package (expressed in the <code>dot</code>
   * language), to the given writer.
   * 
   * @param w The writer to write dot language description of the automaton. 
   * @param FST The automaton to visualize.
   * @param node Starting node (subgraph will be visualized unless it's the automaton's root node).
   * @throws IOException Rethrown if an I/O exception occurs. 
   */
  public static void toDot(Writer w, FST FST, int node) throws IOException {
    w.write("digraph Automaton {\n");
    w.write("  rankdir = LR;\n");

    final BitSet visited = new BitSet();

    w.write("  stop [shape=doublecircle,label=\"\"];\n");
    w.write("  initial [shape=plaintext,label=\"\"];\n");
    w.write("  initial -> " + node + "\n\n");

    visitNode(w, 0, FST, node, visited);
    w.write("}\n");
  }

  private static void visitNode(Writer w, int d, FST FST, int s, BitSet visited) throws IOException {
    visited.set(s);
    w.write("  ");
    w.write(Integer.toString(s));

    if (FST.getFlags().contains(FSTFlags.NUMBERS)) {
      int nodeNumber = FST.getRightLanguageCount(s);
      w.write(" [shape=circle,label=\"" + nodeNumber + "\"];\n");
    } else {
      w.write(" [shape=circle,label=\"\"];\n");
    }

    for (int arc = FST.getFirstArc(s); arc != 0; arc = FST.getNextArc(arc)) {
      w.write("  ");
      w.write(Integer.toString(s));
      w.write(" -> ");
      if (FST.isArcTerminal(arc)) {
        w.write("stop");
      } else {
        w.write(Integer.toString(FST.getEndNode(arc)));
      }

      final byte label = FST.getArcLabel(arc);
      w.write(" [label=\"");
      if (Character.isLetterOrDigit(label))
        w.write((char) label);
      else {
        w.write("0x");
        w.write(Integer.toHexString(label & 0xFF));
      }
      w.write("\"");
      if (FST.isArcFinal(arc))
        w.write(" arrowhead=\"tee\"");
      if (FST instanceof ImmutableFST) {
        if (((ImmutableFST) FST).isNextSet(arc)) {
          w.write(" color=\"blue\"");
        }
      }

      w.write("]\n");
    }

    for (int arc = FST.getFirstArc(s); arc != 0; arc = FST.getNextArc(arc)) {
      if (!FST.isArcTerminal(arc)) {
        int endNode = FST.getEndNode(arc);
        if (!visited.get(endNode)) {
          visitNode(w, d + 1, FST, endNode, visited);
        }
      }
    }
  }

  /**
   * Calculate fan-out ratio (how many nodes have a given number of outgoing arcs).  
   * 
   * @param FST The automaton to calculate fanout for.
   * @param root The starting node for calculations.
   * 
   * @return The returned map contains keys for the number of outgoing arcs and
   * an associated value being the number of nodes with that arc number. 
   */
  public static TreeMap<Integer, Integer> calculateFanOuts(final FST FST, int root) {
    final int[] result = new int[256];
    FST.visitInPreOrder(new StateVisitor() {
      public boolean accept(int state) {
        int count = 0;
        for (int arc = FST.getFirstArc(state); arc != 0; arc = FST.getNextArc(arc)) {
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
   * Calculate the size of "right language" for each state in an FST. The right
   * language is the number of sequences encoded from a given node in the automaton.
   * 
   * @param FST The automaton to calculate right language for.
   * @return Returns a map with node identifiers as keys and their right language
   * counts as associated values. 
   */
  public static IntIntHashMap rightLanguageForAllStates(final FST FST) {
    final IntIntHashMap numbers = new IntIntHashMap();

    FST.visitInPostOrder(new StateVisitor() {
      public boolean accept(int state) {
        int thisNodeNumber = 0;
        for (int arc = FST.getFirstArc(state); arc != 0; arc = FST.getNextArc(arc)) {
          thisNodeNumber += (FST.isArcFinal(arc) ? 1 : 0)
              + (FST.isArcTerminal(arc) ? 0 : numbers.get(FST.getEndNode(arc)));
        }
        numbers.put(state, thisNodeNumber);

        return true;
      }
    });

    return numbers;
  }
}
