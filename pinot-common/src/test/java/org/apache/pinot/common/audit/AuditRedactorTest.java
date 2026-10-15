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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


/// Unit tests for [AuditRedactor].
public class AuditRedactorTest {

  private static final String SECRET = "AKIAIOSFODNN7EXAMPLE-SECRET";

  @DataProvider(name = "sensitiveKeys")
  public Object[][] sensitiveKeys() {
    return new Object[][]{
        // Batch-ingestion S3 credentials, as sent in a connection's params.
        {"input.fs.prop.secretKey"}, {"input.fs.prop.accessKey"},
        // AWS / S3.
        {"accessKey"}, {"accessKeyId"}, {"secretKey"}, {"aws.secret.access.key"}, {"sessionToken"},
        {"catalog.s3.auth.storage.secretKey"},
        // Kafka / SASL.
        {"sasl.jaas.config"}, {"ssl.keystore.password"}, {"ssl.truststore.password"},
        // Kafka inline PEM material, which no *private*key* pattern reaches.
        {"ssl.keystore.key"}, {"ssl.keystore.certificate.chain"},
        // Confluent schema registry: a literal "user:password" pair. "auth", not "authorization".
        {"basic.auth.user.info"}, {"stream.kafka.decoder.prop.basic.auth.user.info"},
        // Session material an operator may allow-list as a header.
        {"Cookie"}, {"Set-Cookie"},
        // Warehouses and catalogs.
        {"snowflake.password"}, {"restOauthClientSecret"}, {"privateKey"}, {"passphrase"},
        // Hadoop S3A, GCS, BigQuery, Delta Lake, Pulsar and tier-storage SSE keys.
        {"fs.s3a.access.key"}, {"fs.s3a.secret.key"}, {"fs.gs.auth.service.account.json.keyfile"},
        {"gcpKey"}, {"gcpKeyPath"}, {"sql.bigquery.jsonKey"}, {"delta.ingestion.table.gcs.keyFile"},
        {"delta.ingestion.table.gcs.keyContentBase64"}, {"stream.pulsar.credsFilePath"},
        {"stream.pulsar.authenticationToken"}, {"sseKmsKeyId"}, {"private_key_id"},
        // Confluent Cloud API key, and its schema registry user info under any prefix.
        {"stream.confluent.key"}, {"stream.confluent.secret"}, {"userInfo"},
        // Iceberg REST catalog and Azure.
        {"restAccessKeyId"}, {"storageSecretAccessKey"}, {"storageSessionToken"}, {"clientSecret"},
        {"sasToken"}, {"proxyPassword"},
        // Headers that carry a credential.
        {"Proxy-Authorization"}, {"X-Auth-Token"}, {"X-Startree-Token"},
        // Dotted words, as in Hadoop-style property names.
        {"api.key.value"}, {"private.key.pem"}, {"encryption.key.material"}, {"signing.key"},
        // A credential-bearing prefix is not exempted by a structural last segment or an auth-mode suffix.
        {"secret.sortKey"}, {"auth.partitionKey"}, {"stream.confluent.secret.joinKey"},
        {"my.secret.credentials.source"}, {"kafka.sasl.password.credentials.provider"},
        // Short password names, Azure connection strings, session ids and SAS signatures.
        {"dbPass"}, {"ssl.keypass"}, {"storepass"}, {"connectionString"}, {"azure.storage.connection.string"},
        {"sessionId"}, {"JSESSIONID"}, {"sig"}, {"x-ms-sig"},
        // Generic.
        {"password"}, {"PASSWORD"}, {"myPassword"}, {"passwd"}, {"pwd"}, {"credentials"}, {"apiKey"},
        {"api_key"}, {"keytab"}, {"token"}, {"authorization"}, {"signature"}, {"keyPath"}
    };
  }

  @DataProvider(name = "benignKeys")
  public Object[][] benignKeys() {
    return new Object[][]{
        {"tableName"}, {"primaryKeyColumns"}, {"partitionKey"}, {"sortedColumn"}, {"user"}, {"username"},
        {"principal"}, {"endpoint"}, {"region"}, {"bucketName"}, {"roleArn"}, {"design"}, {"bypassCache"},
        // Structural names that merely end in "key".
        {"primaryKey"}, {"sortKey"}, {"joinKey"}, {"groupByKey"}, {"dimensionKey"}, {"metricKey"},
        {"timeColumnKey"}, {"schemaRegistryKey"}, {"segment.partitionKey"},
        // Authentication modes, which say how a caller authenticated without being a credential.
        {"stream.kafka.decoder.prop.basic.auth.credentials.source"}, {"fs.s3a.aws.credentials.provider"}
    };
  }

  @Test(dataProvider = "sensitiveKeys")
  public void testSensitiveKeysAreRedacted(String key) {
    assertThat(AuditRedactor.isSensitiveKey(key)).as(key).isTrue();
    assertThat(complete(String.format("{\"%s\":\"%s\"}", key, SECRET))).doesNotContain(SECRET);
  }

  @Test(dataProvider = "benignKeys")
  public void testBenignKeysAreKept(String key) {
    assertThat(AuditRedactor.isSensitiveKey(key)).as(key).isFalse();
    assertThat(complete(String.format("{\"%s\":\"visible\"}", key))).contains("visible");
  }

  @Test
  public void testRedactsNestedObject() {
    String body = "{\"connection\":{\"type\":\"CUSTOM_BATCH\",\"params\":{\"input.fs.prop.secretKey\":\"" + SECRET
        + "\",\"input.fs.prop.accessKey\":\"" + SECRET + "\",\"input.fs.className\":\"S3PinotFS\"}}}";

    String redacted = complete(body);

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

    String redacted = complete(body);

    assertThat(redacted).doesNotContain(SECRET);
    assertThat(redacted).contains("kafka");
  }

  @Test
  public void testRedactsTopLevelArray() {
    String redacted = complete("[{\"password\":\"" + SECRET + "\"}]");
    assertThat(redacted).doesNotContain(SECRET);
  }

  @Test
  public void testMasksValueOfSensitiveNameValuePair() {
    String redacted = complete("{\"props\":[{\"key\":\"fs.s3a.secret.key\",\"value\":\"" + SECRET + "\"},"
        + "{\"value\":\"" + SECRET + "\",\"name\":\"password\"},{\"name\":\"region\",\"value\":\"us-east-1\"}]}");

    assertThat(redacted).doesNotContain(SECRET);
    // The name of the setting stays readable, and so does the value of a pair that is not sensitive.
    assertThat(redacted).contains("\"name\":\"password\"", "\"value\":\"us-east-1\"");
  }

  @Test
  public void testTruncatedBodyMasksValueOfSensitiveNameValuePair() {
    String redacted = truncated("[{\"name\":\"password\",\"value\":\"" + SECRET + "\"},{\"key\":\"accessKey\","
        + "\"value\":\"" + SECRET + "\"},{\"name\":\"region\",\"value\":\"us");

    assertThat(redacted).isEqualTo("[{\"name\":\"password\",\"value\":\"*****\"},"
        + "{\"key\":\"*****\",\"value\":\"*****\"},{\"name\":\"region\"}]" + AuditRequestProcessor.TRUNCATION_MARKER);
  }

  @Test
  public void testSensitiveKeyMasksWholeSubtree() {
    String redacted = complete(
        "{\"credentials\":{\"user\":\"svc\",\"value\":\"" + SECRET + "\"},\"region\":\"us-east-1\"}");

    assertThat(redacted).doesNotContain(SECRET).doesNotContain("svc");
    assertThat(redacted).contains("us-east-1");
  }

  @Test
  public void testStripsUrlUserInfoUnderBenignKey() {
    String redacted = complete(
        "{\"schema.registry.url\":\"https://svc:" + SECRET + "@registry.example.com:8081/path\"}");

    assertThat(redacted).doesNotContain(SECRET).doesNotContain("svc:");
    assertThat(redacted).contains("https://" + AuditRedactor.MASKED_VALUE + "@registry.example.com:8081/path");
  }

  @Test
  public void testStripsUrlUserInfoInsideArrays() {
    String redacted = complete("{\"urls\":[\"s3://AKIA:" + SECRET + "@bucket/key\"]}");

    assertThat(redacted).doesNotContain(SECRET);
    assertThat(redacted).contains("@bucket/key");
  }

  @DataProvider(name = "urlsWithUserInfo")
  public Object[][] urlsWithUserInfo() {
    String mask = AuditRedactor.MASKED_VALUE;
    return new Object[][]{
        // A password containing '@' or '/' is masked whole.
        {"https://svc:p@ss@registry:8081", "https://" + mask + "@registry:8081"},
        {"https://svc:pa/ss@host/path", "https://" + mask + "@host/path"},
        // A scheme with a colon, as in JDBC URLs.
        {"jdbc:postgresql://u:pw@db:5432/x", "jdbc:postgresql://" + mask + "@db:5432/x"},
        // Every URL of a list, and a URL that is not the whole value.
        {"https://u1:p1@h1,https://u2:p2@h2", "https://" + mask + "@h1,https://" + mask + "@h2"},
        {"--uri https://u:p@h --region x", "--uri https://" + mask + "@h --region x"}
    };
  }

  @Test(dataProvider = "urlsWithUserInfo")
  public void testStripsUrlUserInfo(String value, String expected) {
    assertThat(AuditRedactor.redactUrlUserInfo(value)).isEqualTo(expected);
  }

  @Test
  public void testValueWithoutUrlUserInfoIsKept() {
    assertThat(AuditRedactor.redactUrlUserInfo("https://host:8081/a")).isEqualTo("https://host:8081/a");
    assertThat(AuditRedactor.redactUrlUserInfo("user@example.com")).isEqualTo("user@example.com");
  }

  @Test
  public void testUrlWithAtSignInPathMasksTheHostToo() {
    // Deliberate over-redaction: the userinfo match runs to the last '@', so that '@' and '/' in passwords
    // are covered, and a path containing '@' is indistinguishable from that.
    assertThat(AuditRedactor.redactUrlUserInfo("https://host:8081/a@b"))
        .isEqualTo("https://" + AuditRedactor.MASKED_VALUE + "@b");
  }

  @Test
  public void testRootScalarRecordedBySizeOnly() {
    // A bare value has no key to judge it by, so it could be a credential on its own.
    String body = "\"" + SECRET + "\"";

    assertThat(complete(body)).isEqualTo("[redacted: unparseable payload, " + body.length()
        + " bytes]");
    assertThat(complete("12345")).isEqualTo("[redacted: unparseable payload, 5 bytes]");
  }

  @Test
  public void testUnparseableBodyRecordedBySizeOnly() {
    String body = "accessKey=" + SECRET + "&region=us-west-2";

    String redacted = complete(body);

    assertThat(redacted).doesNotContain(SECRET);
    assertThat(redacted).doesNotContain("us-west-2");
    assertThat(redacted).isEqualTo("[redacted: unparseable payload, " + body.length() + " bytes]");
  }

  @Test
  public void testTruncatedBodyKeepsRedactedPrefix() {
    String body = "{\"tableName\":\"t\",\"streamConfigs\":{\"sasl.jaas.config\":\"" + SECRET
        + "\",\"url\":\"https://svc:" + SECRET + "@registry:8081\",\"stream.kafka.topic.name\":\"to";

    // The cut falls inside the topic name, which is dropped; the objects it left open are closed.
    assertThat(truncated(body)).isEqualTo("{\"tableName\":\"t\",\"streamConfigs\":{\"sasl.jaas.config\":\"*****\","
        + "\"url\":\"https://*****@registry:8081\"}}" + AuditRequestProcessor.TRUNCATION_MARKER);
  }

  @Test
  public void testTruncatedInsideSensitiveValueKeepsTheMask() {
    // The key is complete, so it is masked even though its value never arrived in full.
    assertThat(truncated("{\"a\":1,\"password\":\"" + SECRET.substring(0, 5)))
        .isEqualTo("{\"a\":1,\"password\":\"*****\"}" + AuditRequestProcessor.TRUNCATION_MARKER);
    assertThat(truncated("{\"credentials\":{\"user\":\"svc\",\"value\":\"" + SECRET.substring(0, 5)))
        .isEqualTo("{\"credentials\":\"*****\"}" + AuditRequestProcessor.TRUNCATION_MARKER);
  }

  @Test
  public void testTruncatedValueIsDroppedNotShortened() {
    // A number cut short would otherwise be recorded as a different, complete number.
    assertThat(truncated("{\"a\":1,\"b\":12")).isEqualTo("{\"a\":1}" + AuditRequestProcessor.TRUNCATION_MARKER);
    // A complete value just before the cut is kept.
    assertThat(truncated("[\"x\",12,tru")).isEqualTo("[\"x\",12]" + AuditRequestProcessor.TRUNCATION_MARKER);
    // A key cut short is dropped together with its value.
    assertThat(truncated("{\"a\":1,\"passw")).isEqualTo("{\"a\":1}" + AuditRequestProcessor.TRUNCATION_MARKER);
  }

  @Test
  public void testTruncatedAfterCompleteValueKeepsTheMarker() {
    assertThat(truncated("{\"a\":1}")).isEqualTo("{\"a\":1}" + AuditRequestProcessor.TRUNCATION_MARKER);
  }

  @Test
  public void testTruncatedNonJsonRecordedBySizeOnly() {
    String body = "accessKey=" + SECRET + "&region=us";

    assertThat(truncated(body)).isEqualTo(
        "[redacted: payload truncated at " + body.length() + " captured bytes, not parseable]");
    // A bare scalar has no key to judge it by, truncated or not.
    assertThat(truncated("\"" + SECRET)).startsWith("[redacted: payload truncated at");
  }

  @Test
  public void testConcatenatedJsonIsNotSilentlyTruncated() {
    // Trailing content would otherwise be dropped, hiding the second document and its secret.
    String body = "{\"a\":1}{\"password\":\"" + SECRET + "\"}";

    String redacted = complete(body);

    assertThat(redacted).doesNotContain(SECRET);
    assertThat(redacted).isEqualTo("[redacted: unparseable payload, " + body.length() + " bytes]");
  }

  @Test
  public void testSizeIsCountedInBytesNotCharacters() {
    String body = "🔑=secret";
    assertThat(complete(body)).isEqualTo("[redacted: unparseable payload, 11 bytes]");
  }

  @Test
  public void testNullEmptyAndBlankBody() {
    assertThat(AuditRedactor.redactBody(null)).isNull();
    assertThat(complete("")).isNull();
    // Whitespace parses to a MissingNode, which would otherwise serialize to the literal string "null".
    assertThat(complete("   ")).isNull();
  }

  @Test
  public void testSizeIsTheCapturedByteCount() {
    // The size comes from the capture, not from re-encoding a text whose last character may have been split.
    assertThat(AuditRedactor.redactBody(new AuditRequestProcessor.CapturedBody("abc\uFFFD", 5, false)))
        .isEqualTo("[redacted: unparseable payload, 5 bytes]");
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
  public void testRedactMapStripsUrlUserInfo() {
    Map<String, Object> params = new HashMap<>();
    params.put("uri", "https://svc:" + SECRET + "@host/path");
    params.put("uris", Arrays.asList("https://svc:" + SECRET + "@host/a", "https://host/b"));

    Map<String, Object> redacted = AuditRedactor.redact(params);

    assertThat(redacted.toString()).doesNotContain(SECRET);
    assertThat(redacted).containsEntry("uri", "https://" + AuditRedactor.MASKED_VALUE + "@host/path");
    assertThat(redacted).containsEntry("uris",
        Arrays.asList("https://" + AuditRedactor.MASKED_VALUE + "@host/a", "https://host/b"));
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

  private static String complete(String body) {
    return AuditRedactor.redactBody(body == null ? null
        : new AuditRequestProcessor.CapturedBody(body, body.getBytes(StandardCharsets.UTF_8).length, false));
  }

  private static String truncated(String body) {
    return AuditRedactor.redactBody(
        new AuditRequestProcessor.CapturedBody(body, body.getBytes(StandardCharsets.UTF_8).length, true));
  }
}
