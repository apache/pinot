package org.apache.pinot.sql.parsers.rewriter;

import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class RlsFiltersRewriter implements QueryRewriter {

  private static final String ROW_FILTERS = "rowFilters";

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    if (MapUtils.isEmpty(queryOptions)) {
      return pinotQuery;
    }
    String tableName = pinotQuery.getDataSource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    String rowFilters = queryOptions.get(rawTableName);

    if (Strings.isEmpty(rowFilters)) {
      return pinotQuery;
    }

    Expression expression = CalciteSqlParser.compileToExpression(rowFilters);
    Expression existingFilterExpression = pinotQuery.getFilterExpression();
    if (existingFilterExpression != null) {
      Expression combinedFilterExpression =
          RequestUtils.getFunctionExpression(FilterKind.AND.name(), List.of(expression, existingFilterExpression));
      pinotQuery.setFilterExpression(combinedFilterExpression);
    } else {
      pinotQuery.setFilterExpression(expression);
    }
    return pinotQuery;
  }
}
