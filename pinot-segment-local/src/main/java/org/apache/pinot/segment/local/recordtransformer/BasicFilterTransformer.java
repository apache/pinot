package org.apache.pinot.segment.local.recordtransformer;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters records based on a configured filter expression.
 *
 * This transformer evaluates each record using the provided filter configuration
 * and decides whether the record should be kept or filtered out.
 */
public class BasicFilterTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicFilterTransformer.class);

  private final FunctionEvaluator _evaluator;
  private String _filterFunction;

  public BasicFilterTransformer(FilterConfig filterConfig) {
    String filterFunction = null;
    if (filterConfig != null) {
      filterFunction = filterConfig.getFilterFunction();
      _filterFunction = filterConfig.getFilterFunction();
    }
    _evaluator = (filterFunction != null) ? FunctionEvaluatorFactory.getExpressionEvaluator(filterFunction) : null;
  }

  @Override
  public List<GenericRow> transform(List<GenericRow> records) {
    assert _evaluator != null;
    List<GenericRow> filteredRecords = new ArrayList<>();
    for (GenericRow record : records) {
      try {
        if (!Boolean.TRUE.equals(_evaluator.evaluate(record))) {
          filteredRecords.add(record);
        }
      } catch (Exception e) {
        LOGGER.debug("Caught exception while executing filter function: {} for record: {}", _filterFunction,
            record, e);
      }
    }
    return filteredRecords;
  }
}