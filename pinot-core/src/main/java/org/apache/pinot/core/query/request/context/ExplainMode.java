package org.apache.pinot.core.query.request.context;

import org.apache.pinot.core.plan.PinotExplainedRelNode;


public enum ExplainMode {
  /**
   * Explain is disabled, queries are executed normally.
   */
  NONE,
  /**
   * The mode used in single-stage.
   * <p>
   * Each node is returned as a row with the following columns:
   * <ol>
   *   <li>Operator: a String column with descriptions like {@code BROKER_REDUCE(limit:10)}</li>
   *   <li>Operator_Id: an int id used to rebuild the parent relation</li>
   *   <li>Parent_Id: an int id used to rebuild the parent relation</li>
   * </ol>
   */
  DESCRIPTION,
  /**
   * Information is returned in a {@link PinotExplainedRelNode.Info} object.
   * <p>
   * Each segment is returned as a row with the following columns:
   * <ol>
   *   <li>plan: The plan in {@link PinotExplainedRelNode.Info}, encoded as JSON</li>
   * </ol>
   */
  NODE
}
