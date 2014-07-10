package com.linkedin.pinot.query.pruner;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.query.request.SelectionSort;


/**
 * An implementation of SegmentPruner.
 * Querying columns not appearing in the given segment will be pruned.
 * 
 * @author xiafu
 *
 */
public class DataSchemaSegmentPruner implements SegmentPruner {

  @Override
  public boolean prune(IndexSegment segment, Query query) {
    Schema schema = segment.getSegmentMetadata().getSchema();
    if (query.getQueryType().hasSelection()) {
      // Check selection columns
      for (String columnName : query.getSelections().getSelectionColumns()) {
        if ((!columnName.equalsIgnoreCase("*")) && (!schema.isExisted(columnName))) {
          return true;
        }
      }

      // Check columns to do sorting,
      for (SelectionSort selectionOrder : query.getSelections().getSelectionSortSequence()) {
        if (!schema.isExisted(selectionOrder.getColumn())) {
          return true;
        }
      }
    }
    // Check groupBy columns.
    if (query.getQueryType().hasGroupBy()) {
      for (String columnName : query.getGroupBy().getColumns()) {
        if (!schema.isExisted(columnName)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void init(Configuration config) {

  }

}
