package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.core.data.manager.offline.InMemoryTable;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class InMemoryLookupJoinTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "InMemoryLookUpJoin";

  private final List<String> _joinKeys = new ArrayList<>();
  private final List<FieldSpec> _joinValueFieldSpecs = new ArrayList<>();
  private final List<TransformFunction> _joinValueFunctions = new ArrayList<>();

  private HashMap<PrimaryKey, Object[]> _keyValuesMap;
  private String _filterFunc;

  private TransformFunction _condCol1;

  private String _condCol2;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap, QueryContext context) {
    // Check that there are correct number of arguments
    Preconditions.checkArgument(arguments.size() >= 4,
        "At least 4 arguments are required for LOOKUP transform function: "
            + "LOOKUPJOIN(inMemoryTableName, joinKey1, joinKey2, filterFunc, condCol1, condCol2)");

    TransformFunction inMemoryTableFunc = arguments.get(0);
    Preconditions.checkArgument(inMemoryTableFunc instanceof LiteralTransformFunction,
        "First argument must be a literal(string) representing the dimension table name");
    // Lookup parameters
    InMemoryTable inMemoryTable = context.getInMemoryTable(((LiteralTransformFunction) inMemoryTableFunc).getLiteral());

    // Only one join key is allowed.
    List<TransformFunction> joinArguments = arguments.subList(1, 3);
    System.out.println("liuyao joinArguments size:" + joinArguments.size());
    int numJoinArguments = joinArguments.size();
    for (int i = 0; i < numJoinArguments / 2; i++) {
      TransformFunction factJoinValueFunction = joinArguments.get((i * 2));
      TransformResultMetadata factJoinValueFunctionResultMetadata = factJoinValueFunction.getResultMetadata();
      Preconditions.checkArgument(factJoinValueFunctionResultMetadata.isSingleValue(),
          "JoinValue argument must be a single value expression");
      _joinValueFunctions.add(factJoinValueFunction);
      TransformFunction inMemoryJoinKey = joinArguments.get((i * 2 + 1));
      Preconditions.checkArgument(inMemoryJoinKey instanceof LiteralTransformFunction,
          "JoinKey argument must be a literal(string)");
      _joinKeys.add(((LiteralTransformFunction) dimJoinKeyFunction).getLiteral());
      System.out.println("liuyao dimJoinKeyFunction:" + ((LiteralTransformFunction) dimJoinKeyFunction).getLiteral());
      System.out.println("liuyao joinKeys size" +_joinKeys.size());
    }

    // Validate lookup table and relevant columns

    for (String joinKey : _joinKeys) {
      FieldSpec pkColumnSpec = _dataManager.getColumnFieldSpec(joinKey);
      Preconditions.checkArgument(pkColumnSpec != null, "Primary key column doesn't exist in dimension table: %s:%s",
          dimTableName, joinKey);
      _joinValueFieldSpecs.add(pkColumnSpec);
    }

    List<String> tablePrimaryKeyColumns = _dataManager.getPrimaryKeyColumns();
    Preconditions.checkArgument(_joinKeys.equals(tablePrimaryKeyColumns),
        "Provided join keys (%s) must be the same as table primary keys: %s", _joinKeys, tablePrimaryKeyColumns);

    _filterFunc = ((LiteralTransformFunction) arguments.get(3)).getLiteral();

    _condCol1 = arguments.get(4);

    System.out.println("liuyao:" + _condCol1);

    _condCol2 = ((LiteralTransformFunction) arguments.get(5)).getLiteral();
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  private boolean isCondSatisfied(String condFunc, double arg1, double arg2) {
    if (condFunc.equals("GreaterThan")) {
      return arg1 > arg2;
    }
    if (condFunc.equals("SmallerThan")){
      return arg1 < arg2;
    }
    throw new IllegalStateException("Unknown condFunc:" + condFunc);
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {

    int numDocs = projectionBlock.getNumDocs();
    if (_intValuesSV == null) {
      _intValuesSV = new int[numDocs];
    }
    // Get primary key
    int numPkColumns = _joinKeys.size();
    int numDocuments = projectionBlock.getNumDocs();
    Object[] pkColumns = new Object[numPkColumns];
    for (int c = 0; c < numPkColumns; c++) {
      FieldSpec.DataType storedType = _joinValueFieldSpecs.get(c).getDataType().getStoredType();
      TransformFunction tf = _joinValueFunctions.get(c);
      switch (storedType) {
        case INT:
          pkColumns[c] = tf.transformToIntValuesSV(projectionBlock);
          break;
        case LONG:
          pkColumns[c] = tf.transformToLongValuesSV(projectionBlock);
          break;
        case FLOAT:
          pkColumns[c] = tf.transformToFloatValuesSV(projectionBlock);
          break;
        case DOUBLE:
          pkColumns[c] = tf.transformToDoubleValuesSV(projectionBlock);
          break;
        case STRING:
          pkColumns[c] = tf.transformToStringValuesSV(projectionBlock);
          break;
        case BYTES:
          pkColumns[c] = tf.transformToBytesValuesSV(projectionBlock);
          break;
        default:
          throw new IllegalStateException("Unknown column type for primary key");
      }
    }

    double[] condCol1 = new double[numDocs];
    // Get filter variable
    FieldSpec.DataType storedType = _condCol1.getResultMetadata().getDataType().getStoredType();
    System.out.println("condCol1: colm:" + ((IdentifierTransformFunction) _condCol1).getColumnName());
    switch (storedType) {
//      case INT:
//        condCol1 = _condCol1.transformToIntValuesSV(projectionBlock);
//        break;
//      case LONG:
//        condCol1 = _condCol1.transformToLongValuesSV(projectionBlock);
//        break;
//      case FLOAT:
//        condCol1 = _condCol1.transformToFloatValuesSV(projectionBlock);
//        break;
      case DOUBLE:
        condCol1 = _condCol1.transformToDoubleValuesSV(projectionBlock);
        break;
//      case STRING:
//        condCol1 = _condCol1.transformToStringValuesSV(projectionBlock);
//        break;
      default:
        throw new IllegalStateException("Unknown supported type for condCol1");
    }

    Object[] pkValues = new Object[numPkColumns];
    PrimaryKey primaryKey = new PrimaryKey(pkValues);
    for (int i = 0; i < numDocuments; i++) {
      // prepare pk
      for (int c = 0; c < numPkColumns; c++) {
        if (pkColumns[c] instanceof int[]) {
          pkValues[c] = ((int[]) pkColumns[c])[i];
        } else if (pkColumns[c] instanceof long[]) {
          pkValues[c] = ((long[]) pkColumns[c])[i];
        } else if (pkColumns[c] instanceof String[]) {
          pkValues[c] = ((String[]) pkColumns[c])[i];
        } else if (pkColumns[c] instanceof float[]) {
          pkValues[c] = ((float[]) pkColumns[c])[i];
        } else if (pkColumns[c] instanceof double[]) {
          pkValues[c] = ((double[]) pkColumns[c])[i];
        } else if (pkColumns[c] instanceof byte[][]) {
          pkValues[c] = new ByteArray(((byte[][]) pkColumns[c])[i]);
        }
      }
      // lookup
      GenericRow row = _dataManager.lookupRowByPrimaryKey(primaryKey);
      _intValuesSV[i] = 0;
      if (row != null) {
        Object condConl2 = row.getValue(_condCol2);
        System.out.println("liuyao primaryKey:" + primaryKey + " condCol1:" + condCol1[i] + "  condConl2:" + condConl2);

        if (isCondSatisfied(_filterFunc, condCol1[i], ((Number) condConl2).doubleValue())) {
          _intValuesSV[i] = 1;
        }
      }
    }
    return _intValuesSV;
  }
}