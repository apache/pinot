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
package org.apache.pinot.core.operator.filter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link VectorSearchParams} construction and query option parsing.
 */
public class VectorSearchParamsTest {

  @Test
  public void testDefaultParams() {
    VectorSearchParams params = VectorSearchParams.DEFAULT;
    Assert.assertEquals(params.getNprobe(), VectorSearchParams.DEFAULT_NPROBE);
    Assert.assertFalse(params.isExactRerank());
    Assert.assertFalse(params.isMaxCandidatesExplicit());
    Assert.assertEquals(params.getEffectiveMaxCandidates(10), 100);
    Assert.assertEquals(params.getEffectiveMaxCandidates(5), 50);
  }

  @Test
  public void testFromNullQueryOptions() {
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(null);
    Assert.assertSame(params, VectorSearchParams.DEFAULT);
  }

  @Test
  public void testFromEmptyQueryOptions() {
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(Collections.emptyMap());
    Assert.assertSame(params, VectorSearchParams.DEFAULT);
  }

  @Test
  public void testFromQueryOptionsNoVectorKeys() {
    Map<String, String> opts = new HashMap<>();
    opts.put("timeoutMs", "5000");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertSame(params, VectorSearchParams.DEFAULT);
  }

  @Test
  public void testNprobeFromQueryOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_NPROBE, "16");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertEquals(params.getNprobe(), 16);
    Assert.assertFalse(params.isExactRerank());
  }

  @Test
  public void testExactRerankFromQueryOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_EXACT_RERANK, "true");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertTrue(params.isExactRerank());
    Assert.assertEquals(params.getNprobe(), VectorSearchParams.DEFAULT_NPROBE);
  }

  @Test
  public void testMaxCandidatesFromQueryOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_MAX_CANDIDATES, "200");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertTrue(params.isMaxCandidatesExplicit());
    Assert.assertEquals(params.getEffectiveMaxCandidates(10), 200);
    // Max candidates should be at least topK
    Assert.assertEquals(params.getEffectiveMaxCandidates(500), 500);
  }

  @Test
  public void testAllOptionsFromQueryOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_NPROBE, "8");
    opts.put(QueryOptionKey.VECTOR_EXACT_RERANK, "true");
    opts.put(QueryOptionKey.VECTOR_MAX_CANDIDATES, "100");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertEquals(params.getNprobe(), 8);
    Assert.assertTrue(params.isExactRerank());
    Assert.assertEquals(params.getEffectiveMaxCandidates(10), 100);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidNprobe() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_NPROBE, "0");
    VectorSearchParams.fromQueryOptions(opts);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNonNumericNprobe() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_NPROBE, "abc");
    VectorSearchParams.fromQueryOptions(opts);
  }

  @Test
  public void testExactRerankFalseExplicit() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_EXACT_RERANK, "false");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    // "false" is still falsy, so should return DEFAULT since no vector option is truly set
    Assert.assertSame(params, VectorSearchParams.DEFAULT);
  }

  @Test
  public void testToString() {
    VectorSearchParams params = new VectorSearchParams(8, true, 100);
    String s = params.toString();
    Assert.assertTrue(s.contains("nprobe=8"));
    Assert.assertTrue(s.contains("exactRerank=true"));
    Assert.assertTrue(s.contains("maxCandidates=100"));
  }

  @Test
  public void testDefaultMaxCandidatesToString() {
    VectorSearchParams params = new VectorSearchParams(null, false, null);
    String s = params.toString();
    Assert.assertTrue(s.contains("default(topK*10)"));
  }
}
