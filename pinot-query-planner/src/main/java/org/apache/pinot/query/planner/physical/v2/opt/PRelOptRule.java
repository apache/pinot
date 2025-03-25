package org.apache.pinot.query.planner.physical.v2.opt;

import org.apache.pinot.query.planner.physical.v2.PRelNode;


/**
 * Optimization rule for a {@link PRelNode}.
 */
public abstract class PRelOptRule {
  /**
   * Whether an optimization rule should be called for the given {@link PRelNode}.
   */
  public boolean matches(PRelOptRuleCall call) {
    return true;
  }

  /**
   * Allows transforming a {@link PRelNode} into another {@link PRelNode}.
   */
  public abstract PRelNode onMatch(PRelOptRuleCall call);

  /**
   * Called after the subtree rooted at the given {@link PRelNode} is processed completely.
   */
  public PRelNode onDone(PRelNode currentNode) {
    return currentNode;
  }
}
