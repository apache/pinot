package org.apache.pinot.query.planner.physical.v2.opt;

import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import org.apache.pinot.query.planner.physical.v2.PRelNode;


/**
 * An abstract executor for a single physical optimization rule. Implementations can define their own order of how
 * a tree of {@link PRelNode} should be processed.
 */
public abstract class RuleExecutor {
  protected final Deque<PRelNode> _parents = new ArrayDeque<>();

  /**
   * Processes the subtree rooted at currentNode.
   */
  public abstract PRelNode execute(PRelNode currentNode);

  /**
   * Calls {@link #execute(PRelNode)} for a sub-list of inputs of the current node. This ensures that the Deque to
   * track parents is updated accurately.
   */
  protected PRelNode executeForInputs(PRelNode currentNode, int fromIndex, int toIndex) {
    int numInputs = currentNode.getPRelInputs().size();
    Preconditions.checkState(fromIndex <= toIndex && fromIndex >= 0 && toIndex <= numInputs,
        "Invalid input range for PRelNode (fromIndex: %s. toIndex: %s. numInputs: %s", fromIndex, toIndex, numInputs);
    if (fromIndex == toIndex || fromIndex == currentNode.getPRelInputs().size()) {
      return currentNode;
    }
    List<PRelNode> newInputs = new ArrayList<>(currentNode.getPRelInputs());
    _parents.addLast(currentNode);
    for (int index = 0; index < numInputs; index++) {
      if (index >= fromIndex && index < toIndex) {
        PRelNode input = currentNode.getPRelInput(index);
        PRelNode modifiedInput = execute(input);
        if (modifiedInput != input) {
          newInputs.set(index, modifiedInput);
          currentNode = currentNode.copy(Collections.unmodifiableList(newInputs));
          _parents.removeLast();
          _parents.addLast(currentNode);
        }
      }
    }
    _parents.removeLast();
    return currentNode;
  }
}
