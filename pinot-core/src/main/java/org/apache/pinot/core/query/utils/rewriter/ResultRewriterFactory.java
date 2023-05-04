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

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResultRewriterFactory {

  private ResultRewriterFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ResultRewriterFactory.class);
  // left blank intentionally to not load any result rewriter by default
  static final List<String> DEFAULT_RESULT_REWRITERS_CLASS_NAMES = ImmutableList.of();

  static AtomicReference<List<ResultRewriter>> _resultRewriters
      = new AtomicReference<>(getResultRewriter(DEFAULT_RESULT_REWRITERS_CLASS_NAMES));

  public static void init(String resultRewritersClassNamesStr) {
    List<String> resultRewritersClassNames =
        (resultRewritersClassNamesStr != null) ? Arrays.asList(resultRewritersClassNamesStr.split(","))
            : DEFAULT_RESULT_REWRITERS_CLASS_NAMES;
    _resultRewriters.set(getResultRewriter(resultRewritersClassNames));
  }

  public static List<ResultRewriter> getResultRewriter() {
    return _resultRewriters.get();
  }

  private static List<ResultRewriter> getResultRewriter(List<String> resultRewriterClasses) {
    final ImmutableList.Builder<ResultRewriter> builder = ImmutableList.builder();
    for (String resultRewriterClassName : resultRewriterClasses) {
      try {
        builder.add(getResultRewriter(resultRewriterClassName));
      } catch (Exception e) {
        LOGGER.error("Failed to load resultRewriter: {}", resultRewriterClassName, e);
      }
    }
    return builder.build();
  }

  private static ResultRewriter getResultRewriter(String resultRewriterClassName)
      throws Exception {
    final Class<ResultRewriter> resultRewriterClass = (Class<ResultRewriter>) Class.forName(resultRewriterClassName);
    return (ResultRewriter) resultRewriterClass.getDeclaredConstructors()[0].newInstance();
  }
}
