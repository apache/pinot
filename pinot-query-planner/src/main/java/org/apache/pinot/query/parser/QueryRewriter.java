package org.apache.pinot.query.parser;

import org.apache.pinot.query.context.PlannerContext;
import org.apache.calcite.sql.SqlNode;

// TODO convert this into visitor pattern if necessary.
/**
 * Rewrite query.
 */
class QueryRewriter {

  /**
   * Main entrypoint for rewrite.
   */
  public static SqlNode rewrite(SqlNode sqlNodeRoot, PlannerContext plannerContext) {
    return sqlNodeRoot;
  }
}
