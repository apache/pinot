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
package org.apache.pinot.common.audit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for {@link AuditRedactor}.
 */
public class AuditRedactorTest {

  private static final String SECRET = "AKIAIOSFODNN7EXAMPLE-SECRET";

  @DataProvider(name = "sensitiveKeys")
  public Object[][] sensitiveKeys() {
    return new Object[][]{
        // The keys observed leaking in STC-6446.
        {"input.fs.prop.secretKey"}, {"input.fs.prop.accessKey"},
        // AWS / S3.
        {"accessKey"}, {"accessKeyId"}, {"secretKey"}, {"aws.secret.access.key"}, {"sessionToken"},
        {"catalog.s3.auth.storage.secretKey"},
        // Kafka / SASL.
        {"sasl.jaas.config"}, {"ssl.keystore.password"}, {"ssl.truststore.password"},
        // Warehouses and catalogs.
        {"snowflake.password"}, {"restOauthClientSecret"}, {"privateKey"}, {"passphrase"},
        // Generic.
        {"password"}, {"PASSWORD"}, {"myPassword"}, {"passwd"}, {"pwd"}, {"credentials"}, {"apiKey"},
        {"api_key"}, {"keytab"}, {"token"}, {"authorization"}, {"signature"}
    };
  }

  @DataProvider(name = "benignKeys")
  public Object[][] benignKeys() {
    return new Object[][]{
        {"tableName"}, {"primaryKeyColumns"}, {"partitionKey"}, {"sortedColumn"}, {"user"}, {"username"},
        {"principal"}, {"endpoint"}, {"region"}, {"bucketName"}, {"roleArn"}
    };
  }

  @Test(dataProvider = "sensitiveKeys")
  public void testSensitiveKeysAreRedacted(String key) {
    assertThat(AuditRedactor.isSensitiveKey(key)).as(key).isTrue();
    assertThat(AuditRedactor.redactBody(String.format("{\"%s\":\"%s\"}", key, SECRET))).doesNotContain(SECRET);
  }

  @Test(dataProvider = "benignKeys")
  public void testBenignKeysAreKept(String key) {
    assertThat(AuditRedactor.isSensitiveKey(key)).as(key).isFalse();
    assertThat(AuditRedactor.redactBody(String.format("{\"%s\":\"visible\"}", key))).contains("visible");
  }

  @Test
  public void testRedactsNestedObject() {
    String body = "{\"connection\":{\"type\":\"CUSTOM_BATCH\",\"params\":{\"input.fs.prop.secretKey\":\"" + SECRET
        + "\",\"input.fs.prop.accessKey\":\"" + SECRET + "\",\"input.fs.className\":\"S3PinotFS\"}}}";

    String redacted = AuditRedactor.redactBody(body);

    assertThat(redacted).doesNotContain(SECRET);
    assertThat(redacted).contains(AuditRedactor.MASKED_VALUE);
    // Non-credential context survives, which is the whole point of keeping the body at all.
    assertThat(redacted).contains("CUSTOM_BATCH", "S3PinotFS", "input.fs.prop.secretKey");
  }

  @Test
  public void testRedactsInsideArrays() {
    // Kafka credentials live in streamConfigMaps, a list of maps.
    String body = "{\"ingestionConfig\":{\"streamIngestionConfig\":{\"streamConfigMaps\":"
        + "[{\"streamType\":\"kafka\",\"sasl.jaas.config\":\"" + SECRET + "\"}]}}}";

    String redacted = AuditRedactor.redactBody(body);

    assertThat(redacted).doesNotContain(SECRET);
    assertThat(redacted).contains("kafka");
  }

  @Test
  public void testRedactsTopLevelArray() {
    String redacted = AuditRedactor.redactBody("[{\"password\":\"" + SECRET + "\"}]");
    assertThat(redacted).doesNotContain(SECRET);
  }

  @Test
  public void testUnparseableBodyRecordedBySizeOnly() {
    String body = "accessKey=" + SECRET + "&region=us-west-2";

    String redacted = AuditRedactor.redactBody(body);

    assertThat(redacted).doesNotContain(SECRET);
    assertThat(redacted).doesNotContain("us-west-2");
    assertThat(redacted).isEqualTo("[redacted: unparseable payload, " + body.length() + " bytes]");
  }

  @Test
  public void testTruncatedJsonRecordedBySizeOnly() {
    // readRequestBody appends a truncation marker, which leaves the body un-parseable.
    String body = "{\"password\":\"" + SECRET + AuditRequestProcessor.TRUNCATION_MARKER;

    String redacted = AuditRedactor.redactBody(body);

    assertThat(redacted).doesNotContain(SECRET);
    assertThat(redacted).startsWith("[redacted: unparseable payload,");
  }

  @Test
  public void testSizeIsCountedInBytesNotCharacters() {
    String body = "🔑=secret";
    assertThat(AuditRedactor.redactBody(body)).isEqualTo("[redacted: unparseable payload, 11 bytes]");
  }

  @Test
  public void testNullAndEmptyBody() {
    assertThat(AuditRedactor.redactBody(null)).isNull();
    assertThat(AuditRedactor.redactBody("")).isNull();
  }

  @Test
  public void testRedactMapMasksValuesAndPreservesOthers() {
    Map<String, Object> params = new HashMap<>();
    params.put("tableName", "myTable");
    params.put("accessToken", SECRET);
    params.put("tokens", Arrays.asList(SECRET, SECRET));

    Map<String, Object> redacted = AuditRedactor.redact(params);

    assertThat(redacted).containsEntry("tableName", "myTable");
    assertThat(redacted).containsEntry("accessToken", AuditRedactor.MASKED_VALUE);
    // Multi-valued entries are replaced wholesale, so the value count is not disclosed either.
    assertThat(redacted).containsEntry("tokens", AuditRedactor.MASKED_VALUE);
  }

  @Test
  public void testRedactMapDoesNotMutateInput() {
    Map<String, Object> params = new HashMap<>();
    params.put("password", SECRET);

    AuditRedactor.redact(params);

    assertThat(params).containsEntry("password", SECRET);
  }

  @Test
  public void testRedactNullAndEmptyMap() {
    assertThat(AuditRedactor.redact(null)).isNull();
    assertThat(AuditRedactor.redact(new HashMap<>())).isEmpty();
  }
}
