package com.linkedin.pinot.query.pruner;

import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.query.config.SegmentPrunerConfig;
import com.linkedin.pinot.core.query.pruner.SegmentPrunerService;
import com.linkedin.pinot.core.query.pruner.SegmentPrunerServiceImpl;


public class TestSegmentPruner {

  @Test
  public void testTableNameSegmentPruner() throws ConfigurationException {
    final Configuration segmentPrunerConfig = new PropertiesConfiguration();
    segmentPrunerConfig.addProperty("class", "TableNameSegmentPruner");
    segmentPrunerConfig.addProperty("TableNameSegmentPruner.id", "0");
    final SegmentPrunerService segmentPrunerService =
        new SegmentPrunerServiceImpl(new SegmentPrunerConfig(segmentPrunerConfig));
    Assert.assertFalse(segmentPrunerService.prune(getIndexSegment("mirror", "mirrorTest"),
        getBrokerRequest("mirror", "mirrorTest")));
    Assert.assertFalse(segmentPrunerService.prune(getIndexSegment("mirror", "mirror"),
        getBrokerRequest("mirror", null)));
    Assert.assertFalse(segmentPrunerService.prune(getIndexSegment("mirror", "mirror"),
        getBrokerRequest("mirror", "")));
    Assert.assertFalse(segmentPrunerService.prune(getIndexSegment("mirror", "mirrorTest2"),
        getBrokerRequest("mirror", "*")));
    Assert.assertFalse(segmentPrunerService.prune(getIndexSegment("mirror", "mirrorTest"),
        getBrokerRequest("mirror", "mirrorTest,mirrorTest1,mirrorTest2")));
    Assert.assertFalse(segmentPrunerService.prune(getIndexSegment("mirror", "mirrorTest1"),
        getBrokerRequest("mirror", "mirrorTest,mirrorTest1,mirrorTest2")));
    Assert.assertFalse(segmentPrunerService.prune(getIndexSegment("mirror", "mirrorTest2"),
        getBrokerRequest("mirror", "mirrorTest,mirrorTest1,mirrorTest2")));
    Assert.assertFalse(segmentPrunerService.prune(getIndexSegment("mirror", "default"),
        getBrokerRequest("mirror", "default,mirrorTest,mirrorTest1,mirrorTest2")));

    Assert.assertTrue(segmentPrunerService.prune(getIndexSegment("mirror", "mirrorTest"),
        getBrokerRequest("mirror", null)));
    Assert.assertTrue(segmentPrunerService.prune(getIndexSegment("mirror", "mirrorTest"),
        getBrokerRequest("mirror", "")));
    Assert.assertTrue(segmentPrunerService.prune(getIndexSegment("mirror", "mirrorTest"),
        getBrokerRequest("mirror", "mirrorTest1")));
    Assert.assertTrue(segmentPrunerService.prune(getIndexSegment("mirror", "mirrorTest"),
        getBrokerRequest("mirror1", null)));
    Assert.assertTrue(segmentPrunerService.prune(getIndexSegment("mirror", "mirrorTest"),
        getBrokerRequest("mirror1", "default")));

  }

  private BrokerRequest getBrokerRequest(String resourceName, String tableName) {
    final BrokerRequest brokerRequest = new BrokerRequest();
    final QuerySource querySource = new QuerySource();
    querySource.setResourceName(resourceName);
    querySource.setTableName(tableName);
    brokerRequest.setQuerySource(querySource);
    return brokerRequest;
  }

  private IndexSegment getIndexSegment(final String resourceName, final String tableName) {
    return new IndexSegment() {

      @Override
      public String getSegmentName() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public SegmentMetadata getSegmentMetadata() {
        return new SegmentMetadata() {

          @Override
          public Map<String, String> toMap() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public String getVersion() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public int getTotalDocs() {
            // TODO Auto-generated method stub
            return 0;
          }

          @Override
          public Interval getTimeInterval() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public Duration getTimeGranularity() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public String getTableName() {
            return tableName;
          }

          @Override
          public String getShardingKey() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public Schema getSchema() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public String getResourceName() {
            return resourceName;
          }

          @Override
          public String getName() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public String getIndexType() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public String getIndexDir() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public String getCrc() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public long getIndexCreationTime() {
            // TODO Auto-generated method stub
            return 0;
          }

          @Override
          public long getPushTime() {
            return Long.MIN_VALUE;
          }

          @Override
          public long getRefreshTime() {
            return Long.MIN_VALUE;
          }
        };
      }

      @Override
      public IndexType getIndexType() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public DataSource getDataSource(String columnName, Predicate p) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public DataSource getDataSource(String columnName) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String[] getColumnNames() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String getAssociatedDirectory() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public void destory() {
        // TODO Auto-generated method stub
        
      }
    };
  }
}
