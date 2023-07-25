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
package org.apache.pinot.core.operator.dociditerators;

import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code ExpressionEvaluatesToTrueScanDocIdIterator} returns the document IDs that the predicate evaluates to NULL.
 */
public class ExpressionEvaluatesToNullScanDocIdIterator extends ExpressionScanDocIdIterator {
  public ExpressionEvaluatesToNullScanDocIdIterator(TransformFunction transformFunction,
      Map<String, DataSource> dataSourceMap, int numDocs) {
    super(transformFunction, dataSourceMap, numDocs);
  }

  @Override
  protected void processProjectionBlock(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds) {
    int numDocs = projectionBlock.getNumDocs();
    TransformResultMetadata resultMetadata = _transformFunction.getResultMetadata();
    if (resultMetadata.isSingleValue()) {
      _numEntriesScanned += numDocs;
      RoaringBitmap nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
      if (nullBitmap != null) {
        for (int docId : nullBitmap) {
          matchingDocIds.add(docId);
        }
      }
    } else {
      throw new IllegalStateException();
    }
  }
}
