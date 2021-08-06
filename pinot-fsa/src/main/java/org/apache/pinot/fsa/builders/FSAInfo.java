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
package org.apache.pinot.fsa.builders;

import java.util.BitSet;

import com.carrotsearch.hppc.IntIntHashMap;
import org.apache.pinot.fsa.FSA;
import org.apache.pinot.fsa.FSA5;


/**
 * Compute additional information about an FSA: number of arcs, nodes, etc.
 */
public final class FSAInfo {
  /**
   * Computes the exact number of states and nodes by recursively traversing the FSA.
   */
  private static class NodeVisitor {
    final BitSet visitedArcs = new BitSet();
    final BitSet visitedNodes = new BitSet();

    int nodes;
    int arcs;
    int totalArcs;

    private final FSA fsa;

    NodeVisitor(FSA fsa) {
      this.fsa = fsa;
    }

    public void visitNode(final int node) {
      if (visitedNodes.get(node)) {
        return;
      }
      visitedNodes.set(node);

      nodes++;
      for (int arc = fsa.getFirstArc(node); arc != 0; arc = fsa.getNextArc(arc)) {
        if (!visitedArcs.get(arc)) {
          arcs++;
        }
        totalArcs++;
        visitedArcs.set(arc);

        if (!fsa.isArcTerminal(arc)) {
          visitNode(fsa.getEndNode(arc));
        }
      }
    }
  }

  /**
   * Computes the exact number of final states.
   */
  private static class FinalStateVisitor {
    final IntIntHashMap visitedNodes = new IntIntHashMap();

    private final FSA fsa;

    FinalStateVisitor(FSA fsa) {
      this.fsa = fsa;
    }

    public int visitNode(int node) {
      int index = visitedNodes.indexOf(node);
      if (index >= 0) {
        return visitedNodes.indexGet(index);
      }

      int fromHere = 0;
      for (int arc = fsa.getFirstArc(node); arc != 0; arc = fsa.getNextArc(arc)) {
        if (fsa.isArcFinal(arc))
          fromHere++;

        if (!fsa.isArcTerminal(arc)) {
          fromHere += visitNode(fsa.getEndNode(arc));
        }
      }
      visitedNodes.put(node, fromHere);
      return fromHere;
    }
  }

  /**
   * Number of nodes in the automaton.
   */
  public final int nodeCount;

  /**
   * Number of arcs in the automaton, excluding an arcs from the zero node (initial) and an arc from the start node to
   * the root node.
   */
  public final int arcsCount;

  /**
   * Total number of arcs, counting arcs that physically overlap due to merging.
   */
  public final int arcsCountTotal;

  /**
   * Number of final states (number of input sequences stored in the automaton).
   */
  public final int finalStatesCount;

  /**
   * Arcs size (in serialized form).
   */
  public final int size;

  /*
	 * 
	 */
  public FSAInfo(FSA fsa) {
    final NodeVisitor w = new NodeVisitor(fsa);
    int root = fsa.getRootNode();
    if (root > 0) {
      w.visitNode(root);
    }

    this.nodeCount = 1 + w.nodes;
    this.arcsCount = 1 + w.arcs;
    this.arcsCountTotal = 1 + w.totalArcs;

    final FinalStateVisitor fsv = new FinalStateVisitor(fsa);
    this.finalStatesCount = fsv.visitNode(fsa.getRootNode());

    if (fsa instanceof FSA5) {
      this.size = ((FSA5) fsa).arcs.length;
    } else {
      this.size = 0;
    }
  }

  /*
	 * 
	 */
  public FSAInfo(int nodeCount, int arcsCount, int arcsCountTotal, int finalStatesCount) {
    this.nodeCount = nodeCount;
    this.arcsCount = arcsCount;
    this.arcsCountTotal = arcsCountTotal;
    this.finalStatesCount = finalStatesCount;
    this.size = 0;
  }

  /*
	 * 
	 */
  @Override
  public String toString() {
    return "Nodes: " + nodeCount + ", arcs visited: " + arcsCount + ", arcs total: " + arcsCountTotal
        + ", final states: " + finalStatesCount + ", size: " + size;
  }
}
