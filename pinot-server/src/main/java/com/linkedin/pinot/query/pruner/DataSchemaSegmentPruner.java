package com.linkedin.pinot.query.pruner;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.core.data.Schema;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
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
    if (query.getSelections() != null) {
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
    if (query.getGroupBy() != null) {
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
