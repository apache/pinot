package com.linkedin.pinot.core.query.pruner;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.common.query.request.Query;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * An implementation of SegmentPruner.
 * Querying tables not appearing in the given segment will be pruned.
 * If table name in the query is null or empty string or "default",
 * it will always match all the segments for a specific resource.
 * 
 * @author xiafu
 *
 */
public class TableNameSegmentPruner implements SegmentPruner {

  @Override
  public boolean prune(IndexSegment segment, Query query) {
    if (query.getTableName() == null || query.getTableName().equals("") || query.getTableName().equals("default")) {
      return false;
    }
    String[] tableNames = query.getTableName().split(",");
    for (String tableName : tableNames) {
      if (tableName.equals(segment.getSegmentMetadata().getTableName())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void init(Configuration config) {

  }

}
