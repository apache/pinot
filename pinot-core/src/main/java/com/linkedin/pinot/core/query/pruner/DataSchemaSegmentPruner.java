package com.linkedin.pinot.core.query.pruner;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * An implementation of SegmentPruner.
 * Querying columns not appearing in the given segment will be pruned.
 * 
 * @author xiafu
 *
 */
public class DataSchemaSegmentPruner implements SegmentPruner {

  @Override
  public boolean prune(IndexSegment segment, BrokerRequest brokerRequest) {
    Schema schema = segment.getSegmentMetadata().getSchema();
    if (brokerRequest.getSelections() != null) {
      // Check selection columns
      for (String columnName : brokerRequest.getSelections().getSelectionColumns()) {
        if ((!columnName.equalsIgnoreCase("*")) && (!schema.isExisted(columnName))) {
          return true;
        }
      }

      // Check columns to do sorting,
      for (SelectionSort selectionOrder : brokerRequest.getSelections().getSelectionSortSequence()) {
        if (!schema.isExisted(selectionOrder.getColumn())) {
          return true;
        }
      }
    }
    // Check groupBy columns.
    if (brokerRequest.getGroupBy() != null) {
      for (String columnName : brokerRequest.getGroupBy().getColumns()) {
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
