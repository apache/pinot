/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.query.config.SegmentPrunerConfig;
import com.linkedin.pinot.core.query.pruner.SegmentPrunerService;
import com.linkedin.pinot.core.query.pruner.SegmentPrunerServiceImpl;


public class TestSegmentPruner {

  private BrokerRequest getBrokerRequest(String resourceName) {
    final BrokerRequest brokerRequest = new BrokerRequest();
    final QuerySource querySource = new QuerySource();
    querySource.setResourceName(resourceName);
    brokerRequest.setQuerySource(querySource);
    return brokerRequest;
  }

  private IndexSegment getIndexSegment(final String resourceName) {
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

          @Override
          public boolean hasDictionary(String columnName) {
            // TODO Auto-generated method stub
            return false;
          }

          @Override
          public boolean close() {
            // TODO Auto-generated method stub
            return false;
          }
        };
      }

      @Override
      public IndexType getIndexType() {
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
      public void destroy() {
        // TODO Auto-generated method stub
      }

      @Override
      public int getTotalDocs() {
        // TODO Auto-generated method stub
        return 0;
      }
    };
  }
}
