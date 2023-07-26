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
package org.apache.pinot.core.query.utils.rewriter;

import java.util.List;
import org.apache.pinot.common.utils.DataSchema;


public class ResultRewriteUtils {

  private ResultRewriteUtils() {
  }

  public static RewriterResult rewriteResult(DataSchema dataSchema, List<Object[]> rows) {
    for (ResultRewriter resultRewriter : ResultRewriterFactory.getResultRewriter()) {
      RewriterResult result = resultRewriter.rewrite(dataSchema, rows);
      dataSchema = result.getDataSchema();
      rows = result.getRows();
    }
    return new RewriterResult(dataSchema, rows);
  }
}
