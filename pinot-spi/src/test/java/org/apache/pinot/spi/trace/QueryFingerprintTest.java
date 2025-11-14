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
package org.apache.pinot.spi.trace;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryFingerprintTest {

  @Test
  public void testConstructorAndGetters() {
    String fingerprint = "SELECT col1 FROM myTable WHERE col2 = ?";
    String queryHash = "abc123def456";

    QueryFingerprint qf = new QueryFingerprint(queryHash, fingerprint);

    Assert.assertNotNull(qf);
    Assert.assertEquals(qf.getFingerprint(), fingerprint);
    Assert.assertEquals(qf.getQueryHash(), queryHash);
  }

  @Test
  public void testWithEmptyStrings() {
    String fingerprint = "";
    String queryHash = "";

    QueryFingerprint qf = new QueryFingerprint(queryHash, fingerprint);

    Assert.assertNotNull(qf);
    Assert.assertEquals(qf.getFingerprint(), "");
    Assert.assertEquals(qf.getQueryHash(), "");
  }

  @Test
  public void testWithNullFingerprint() {
    String fingerprint = null;
    String queryHash = "abc123";

    QueryFingerprint qf = new QueryFingerprint(queryHash, fingerprint);

    Assert.assertNotNull(qf);
    Assert.assertNull(qf.getFingerprint());
    Assert.assertEquals(qf.getQueryHash(), queryHash);
  }

  @Test
  public void testWithNullQueryHash() {
    String fingerprint = "SELECT * FROM table";
    String queryHash = null;

    QueryFingerprint qf = new QueryFingerprint(queryHash, fingerprint);

    Assert.assertNotNull(qf);
    Assert.assertEquals(qf.getFingerprint(), fingerprint);
    Assert.assertNull(qf.getQueryHash());
  }

  @Test
  public void testWithBothNull() {
    QueryFingerprint qf = new QueryFingerprint(null, null);

    Assert.assertNotNull(qf);
    Assert.assertNull(qf.getFingerprint());
    Assert.assertNull(qf.getQueryHash());
  }

  @Test
  public void testMultipleInstances() {
    QueryFingerprint qf1 = new QueryFingerprint("hash1", "fp1");
    QueryFingerprint qf2 = new QueryFingerprint("hash2", "fp2");
    QueryFingerprint qf3 = new QueryFingerprint("hash1", "fp1");

    Assert.assertNotSame(qf1, qf2);
    Assert.assertNotSame(qf1, qf3);

    Assert.assertEquals(qf1.getFingerprint(), qf3.getFingerprint());
    Assert.assertEquals(qf1.getQueryHash(), qf3.getQueryHash());

    Assert.assertNotEquals(qf1.getFingerprint(), qf2.getFingerprint());
    Assert.assertNotEquals(qf1.getQueryHash(), qf2.getQueryHash());
  }
}
