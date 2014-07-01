package com.linkedin.pinot.query.pruner;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.request.Query;


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
      for (String columnName : query.getSelections().getSelectionColumns()) {
        if (!schema.isExisted(columnName)) {
          return true;
        }
      }

      for (String columnName : query.getSelections().getOrderBySequence()) {
        if (!schema.isExisted(columnName)) {
          return true;
        }
      }
    }
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
