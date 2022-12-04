package org.apache.pinot.sql.parsers.rewriter.join;

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;


public class RightJoinRewriter extends SqlShuttle {

  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) call;
      if (join.getJoinType() == JoinType.RIGHT) {
        return super.visit(new SqlJoin(
            call.getParserPosition(),
            join.getRight(),
            join.isNaturalNode(),
            SqlLiteral.createSymbol(JoinType.LEFT, join.getJoinTypeNode().getParserPosition()),
            join.getLeft(),
            join.getConditionTypeNode(),
            join.getCondition()));
      }
    }
    return super.visit(call);
  }
}