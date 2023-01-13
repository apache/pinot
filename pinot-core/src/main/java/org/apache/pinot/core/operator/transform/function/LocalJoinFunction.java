package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class LocalJoinFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "localJoin";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are correct number of arguments
    Preconditions.checkArgument(arguments.size() >= 4,
        "At least 5 arguments are required for LocalJoin transform function: "
            + "localJoin(TABLE1,  TABLE2, JoinKey1, JoinKey2, JoinCondition, SelectColumns)");
    Preconditions
        .checkArgument(arguments.size() % 2 == 0, "Should have the same number of JoinKey and JoinValue arguments");

    TransformFunction leftTable = arguments.get(0);
    Preconditions.checkArgument(leftTable instanceof LiteralTransformFunction,
        "First argument must be a literal(string) representing the left table name");

    TransformFunction rightTable = arguments.get(1);
    Preconditions.checkArgument(rightTable instanceof LiteralTransformFunction,
        "Second argument must be a literal(string) representing the right table name");

    // Lookup parameters
    TransformFunction joinKey1 = arguments.get(2);
    Preconditions.checkArgument(joinKey1 instanceof LiteralTransformFunction,
        "Third argument must be a literal(string) representing the left join key");

    TransformFunction joinKey2 = arguments.get(3);
    Preconditions.checkArgument(joinKey2 instanceof LiteralTransformFunction,
        "Forth argument must be a literal(string) representing the right join key");
  }

  @Override
  public TransformResultMetadata getResultMetadata() {

  }
}
