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
import java.util.BitSet;
import org.apache.pinot.segment.local.utils.nativefst.FST;


/**
 * Compute additional information about a FST: number of arcs, nodes, etc.
 */
public final class FSTInfo {
  /**
   * Number of nodes in the automaton.
   */
  public final int _nodeCount;
  /**
   * Number of arcs in the automaton, excluding an arcs from the zero node (initial) and an arc from the start node to
   * the root node.
   */
  public final int _arcsCount;
  /**
   * Total number of arcs, counting arcs that physically overlap due to merging.
   */
  public final int _arcsCountTotal;
  /**
   * Number of final states (number of input sequences stored in the automaton).
   */
  public final int _finalStatesCount;

  /*
   *
   */
  public FSTInfo(FST FST) {
    final NodeVisitor w = new NodeVisitor(FST);
    int root = FST.getRootNode();
    if (root > 0) {
      w.visitNode(root);
    }

    this._nodeCount = 1 + w._nodes;
    this._arcsCount = 1 + w._arcs;
    this._arcsCountTotal = 1 + w._totalArcs;

    final FinalStateVisitor fsv = new FinalStateVisitor(FST);
    this._finalStatesCount = fsv.visitNode(FST.getRootNode());
  }

  /*
   *
   */
  @Override
  public String toString() {
    return "Nodes: " + _nodeCount + ", arcs visited: " + _arcsCount + ", arcs total: " + _arcsCountTotal
        + ", final states: " + _finalStatesCount;
  }

  /**
   * Computes the exact number of states and nodes by recursively traversing the FST.
   */
  private static class NodeVisitor {
    final BitSet _visitedArcs = new BitSet();
    final BitSet _visitedNodes = new BitSet();
    private final FST _fst;
    int _nodes;
    int _arcs;
    int _totalArcs;

    NodeVisitor(FST fst) {
      this._fst = fst;
    }

    public void visitNode(final int node) {
      if (_visitedNodes.get(node)) {
        return;
      }
      _visitedNodes.set(node);

      _nodes++;
      for (int arc = _fst.getFirstArc(node); arc != 0; arc = _fst.getNextArc(arc)) {
        if (!_visitedArcs.get(arc)) {
          _arcs++;
        }
        _totalArcs++;
        _visitedArcs.set(arc);

        if (!_fst.isArcTerminal(arc)) {
          visitNode(_fst.getEndNode(arc));
        }
      }
    }
  }

  /**
   * Computes the exact number of final states.
   */
  private static class FinalStateVisitor {
    final IntIntHashMap _visitedNodes = new IntIntHashMap();

    private final FST _fst;

    FinalStateVisitor(FST fst) {
      this._fst = fst;
    }

    public int visitNode(int node) {
      int index = _visitedNodes.indexOf(node);
      if (index >= 0) {
        return _visitedNodes.indexGet(index);
      }

      int fromHere = 0;
      for (int arc = _fst.getFirstArc(node); arc != 0; arc = _fst.getNextArc(arc)) {
        if (_fst.isArcFinal(arc)) {
          fromHere++;
        }

        if (!_fst.isArcTerminal(arc)) {
          fromHere += visitNode(_fst.getEndNode(arc));
        }
      }
      _visitedNodes.put(node, fromHere);
      return fromHere;
    }
  }
}
