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
package org.apache.pinot.core.operator.streaming;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.core.operator.InstanceResponseOperator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.trace.Tracing;


public class StreamingInstanceResponseOperator extends InstanceResponseOperator {
  private static final String EXPLAIN_NAME = "STREAMING_INSTANCE_RESPONSE";

  private final BaseStreamingCombineOperator<?> _streamingCombineOperator;
  private final ResultsBlockStreamer _streamer;

  public StreamingInstanceResponseOperator(BaseCombineOperator<?> combinedOperator, List<IndexSegment> indexSegments,
      List<FetchContext> fetchContexts, ResultsBlockStreamer streamer, QueryContext queryContext) {
    super(combinedOperator, indexSegments, fetchContexts, queryContext);
    _streamingCombineOperator =
        combinedOperator instanceof BaseStreamingCombineOperator ? (BaseStreamingCombineOperator<?>) combinedOperator
            : null;
    _streamer = streamer;
  }

  @Override
  protected InstanceResponseBlock getNextBlock() {
    try {
      prefetchAll();
      if (_streamingCombineOperator != null) {
        _streamingCombineOperator.start();
        BaseResultsBlock resultsBlock = _streamingCombineOperator.nextBlock();
        while (!(resultsBlock instanceof MetadataResultsBlock)) {
          if (resultsBlock instanceof ExceptionResultsBlock) {
            return new InstanceResponseBlock(resultsBlock);
          }
          if (resultsBlock.getNumRows() > 0) {
            _streamer.send(resultsBlock);
          }
          resultsBlock = _streamingCombineOperator.nextBlock();
        }
        // Return a metadata-only block in the end
        return new InstanceResponseBlock(resultsBlock);
      } else {
        // Handle single block combine operator in streaming fashion
        BaseResultsBlock resultsBlock = _combineOperator.nextBlock();
        if (resultsBlock instanceof ExceptionResultsBlock) {
          return new InstanceResponseBlock(resultsBlock);
        }
        if (resultsBlock.getNumRows() > 0) {
          _streamer.send(resultsBlock);
        }
        return new InstanceResponseBlock(resultsBlock).toMetadataOnlyResponseBlock();
      }
    } catch (EarlyTerminationException e) {
      Exception killedErrorMsg = Tracing.getThreadAccountant().getErrorStatus();
      return new InstanceResponseBlock(new ExceptionResultsBlock(new QueryCancelledException(
          "Cancelled while streaming results" + (killedErrorMsg == null ? StringUtils.EMPTY : " " + killedErrorMsg),
          e)));
    } catch (Exception e) {
      return new InstanceResponseBlock(new ExceptionResultsBlock(QueryException.INTERNAL_ERROR, e));
    } finally {
      if (_streamingCombineOperator != null) {
        _streamingCombineOperator.stop();
      }
      releaseAll();
    }
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
