package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;


public class IsNotNullTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "IS_NOT_NULL";

  private TransformFunction _leftTransformFunction;
  private int[] _results;
  private Map<String, DataSource> _dataSourceMap = new HashMap<>();

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(arguments.size() == 1,
        "Exact 1 argument is required for IS_NOT_NULL operator function");
    _leftTransformFunction = arguments.get(0);
    if (!(_leftTransformFunction instanceof IdentifierTransformFunction)) {
      throw new IllegalArgumentException(
          "Only column names are supported in IS_NOT_NULL. Support for functions is planned for future release");
    }
    _dataSourceMap = dataSourceMap;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_results == null || _results.length < length) {
      _results = new int[length];
    }

    int[] docIds = projectionBlock.getDocIds();
    String columnName = ((IdentifierTransformFunction) _leftTransformFunction).getColumnName();
    NullValueVectorReader nullValueVector = _dataSourceMap.get(columnName).getNullValueVector();

    if (nullValueVector != null) {
      for (int idx = 0; idx < length; idx++) {
        int docId = docIds[idx];
        if (nullValueVector.isNull(docId)) {
          _results[idx] = 0;
        } else {
          _results[idx] = 1;
        }
      }
    } else {
      Arrays.fill(_results, 1);
    }

    return _results;
  }
}
