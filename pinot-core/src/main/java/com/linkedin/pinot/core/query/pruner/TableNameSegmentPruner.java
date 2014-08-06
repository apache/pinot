package com.linkedin.pinot.core.query.pruner;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * An implementation of SegmentPruner.
 * Querying tables not appearing in the given segment will be pruned.
 * If table name in the brokerRequest is null or empty string or "default",
 * it will always match all the segments for a specific resource.
 * 
 * @author xiafu
 *
 */
public class TableNameSegmentPruner implements SegmentPruner {

  @Override
  public boolean prune(IndexSegment segment, BrokerRequest brokerRequest) {
    if (brokerRequest.getQuerySource() == null || brokerRequest.getQuerySource().getTableName() == null
        || brokerRequest.getQuerySource().getTableName().equals("")
        || brokerRequest.getQuerySource().getTableName().equals("default")) {
      return false;
    }
    String[] tableNames = brokerRequest.getQuerySource().getTableName().split(",");
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
