/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.operator;

import java.util.List;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.slf4j.LoggerFactory;


public abstract class MultiStageOperator extends BaseOperator<TransferableBlock> implements AutoCloseable {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MultiStageOperator.class);

  @Override
  public List<MultiStageOperator> getChildOperators() {
    throw new UnsupportedOperationException();
  }

  // TODO: Ideally close() call should finish within request deadline.
  // TODO: Consider passing deadline as part of the API.
  @Override
  public void close() {
    for (MultiStageOperator op : getChildOperators()) {
      try {
        op.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close operator: " + op + " with exception:" + e);
        // Continue processing because even one operator failed to be close, we should still close the rest.
      }
    }
  }

  public void cancel(Throwable e) {
    for (MultiStageOperator op : getChildOperators()) {
      try {
        op.cancel(e);
      } catch (Exception e2) {
        LOGGER.error("Failed to cancel operator:" + op + "with error:" + e + " with exception:" + e2);
        // Continue processing because even one operator failed to be cancelled, we should still cancel the rest.
      }
    }
  }
}
