package org.apache.pinot.core.operator.filter;

import com.google.common.base.CaseFormat;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Filter operator for Map matching that internally uses JsonMatchFilterOperator.
 * This operator converts map predicates to JSON predicates and delegates filtering operations
 * to JsonMatchFilterOperator.
 */
public class MapIndexFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_MAP_INDEX";

  private final JsonMatchFilterOperator _jsonMatchOperator;
  private final String _columnName;
  private final String _keyName;
  private final Predicate _predicate;
  private final JsonMatchPredicate _jsonMatchPredicate;

  public MapIndexFilterOperator(IndexSegment indexSegment, Predicate predicate, int numDocs) {
    super(numDocs, false);
    _predicate = predicate;

    // Get column name and key name from function arguments
    List<ExpressionContext> arguments = predicate.getLhs().getFunction().getArguments();
    if (arguments.size() != 2) {
      throw new IllegalStateException("Expected two arguments (column name and key name), found: " + arguments.size());
    }

    _columnName = arguments.get(0).getIdentifier();
    _keyName = String.valueOf(arguments.get(1).getLiteral());

    // Convert predicate to JSON format based on type
    String jsonValue;
    switch (predicate.getType()) {
      case EQ:
        jsonValue = createJsonPredicateValue(_keyName, ((EqPredicate) predicate).getValue());
        break;
      case IN:
        jsonValue = createJsonArrayPredicateValue(_keyName, ((InPredicate) predicate).getValues());
        break;
      case RANGE:
        RangePredicate rangePredicate = (RangePredicate) predicate;
        jsonValue =
            createJsonRangePredicateValue(_keyName, rangePredicate.getLowerBound(), rangePredicate.getUpperBound(),
                rangePredicate.isLowerInclusive(), rangePredicate.isUpperInclusive());
        break;
      default:
        throw new IllegalStateException("Unsupported predicate type for map index: " + predicate.getType());
    }

    // Create identifier expression for the JSON column
    ExpressionContext jsonLhs = ExpressionContext.forIdentifier("json");
    _jsonMatchPredicate = new JsonMatchPredicate(jsonLhs, jsonValue);

    // Get JSON index and create operator
    DataSource dataSource = indexSegment.getDataSource(_columnName);

    if (dataSource instanceof MapDataSource) {
      MapDataSource mapDS = (MapDataSource) dataSource;
      DataSource keyDS = mapDS.getKeyDataSource(_keyName);
      JsonIndexReader jsonIndex = keyDS.getJsonIndex();
      if (jsonIndex == null) {
        throw new IllegalStateException("No JSON index found for column: " + _columnName);
      }
      _jsonMatchOperator = new JsonMatchFilterOperator(jsonIndex, _jsonMatchPredicate, numDocs);
    } else {
      throw new IllegalStateException(
          "Expected MapDataSource for column: " + _columnName + ", found: " + dataSource.getClass().getSimpleName());
    }
  }

  private String createJsonPredicateValue(String key, String value) {
    // Format: '"$[0].key" = ''value'''
    // Example: JSON_MATCH(items, '"$[0].price" = ''100''')
    //"$[1].key"='foo'
    return String.format("\"$[0].%s\" = '%s'", key, value);
  }

  private String createJsonArrayPredicateValue(String key, List<String> values) {
    StringBuilder valuesStr = new StringBuilder();
    for (int i = 0; i < values.size(); i++) {
        if (i > 0) {
            valuesStr.append(", ");
        }
        valuesStr.append("''").append(values.get(i)).append("''");
    }
    // Format: '"$[0].key" IN (''value1'', ''value2'')'
    return String.format("\"$[0].%s\" IN (%s)", key, valuesStr);
  }

  private String createJsonRangePredicateValue(String key, String lower, String upper, 
                                             boolean lowerInclusive, boolean upperInclusive) {
    StringBuilder predicate = new StringBuilder();
    if (lower != null) {
        predicate.append(String.format("\"$[0].%s\" %s ''%s''", 
            key, lowerInclusive ? ">=" : ">", lower));
    }
    if (lower != null && upper != null) {
        predicate.append(" AND ");
    }
    if (upper != null) {
        predicate.append(String.format("\"$[0].%s\" %s ''%s''", 
            key, upperInclusive ? "<=" : "<", upper));
    }
    return predicate.toString();
  }

  @Override
  protected BlockDocIdSet getTrues() {
    ImmutableRoaringBitmap bitmap = _jsonMatchOperator.getBitmaps().reduce();
    record(bitmap);
    return new BitmapDocIdSet(bitmap, _numDocs);
  }

  @Override
  public boolean canOptimizeCount() {
    return _jsonMatchOperator.canOptimizeCount();
  }

  @Override
  public int getNumMatchingDocs() {
    return _jsonMatchOperator.getNumMatchingDocs();
  }

  @Override
  public boolean canProduceBitmaps() {
    return _jsonMatchOperator.canProduceBitmaps();
  }

  @Override
  public BitmapCollection getBitmaps() {
    return _jsonMatchOperator.getBitmaps();
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder =
        new StringBuilder(EXPLAIN_NAME).append("(column:").append(_columnName).append(",key:").append(_keyName)
            .append(",indexLookUp:map_index").append(",operator:").append(_predicate.getType()).append(",predicate:")
            .append(_predicate).append(",delegateTo:json_match").append(')');
    return stringBuilder.toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putString("column", _columnName);
    attributeBuilder.putString("key", _keyName);
    attributeBuilder.putString("indexLookUp", "map_index");
    attributeBuilder.putString("operator", _predicate.getType().name());
    attributeBuilder.putString("predicate", _predicate.toString());
    attributeBuilder.putString("delegateTo", "json_match");
  }

  private void record(ImmutableRoaringBitmap bitmap) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
      recording.setColumnName(_columnName);
      recording.setFilter(FilterType.INDEX, _predicate.getType().name());
      recording.setNumDocsMatchingAfterFilter(bitmap.getCardinality());
    }
  }
}
