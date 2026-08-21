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
package org.apache.pinot.spi.config.table;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class TableConfigRedactionUtilsTest {
  private static final String MARKER = TableConfigRedactionUtils.REDACTION_MARKER;
  private static final String STREAM_PASSWORD_KEY = "stream.kafka.consumer.prop.ssl.keystore.password";
  private static final String STREAM_PLACEHOLDER_KEY = "stream.kafka.consumer.prop.ssl.truststore.password";
  private static final String JAAS_KEY = "stream.kafka.consumer.prop.sasl.jaas.config";
  private static final String URI_KEY = "stream.kafka.schema.registry.url";
  private static final String CLIENT_ID_KEY = "client.id";
  private static final String PLACEHOLDER = "${STREAM_TRUSTSTORE_PASSWORD}";
  private static final String URI = "https://uri-user:uri-password@registry.example.test/schemas"
      + "?access_token=uri-token&X-Amz-Signature=uri-signature&region=us-west-2";
  private static final String PLACEHOLDER_URI =
      "https://${URI_USER:default-user}:${URI_PASSWORD:default-pass}@registry.example.test/schemas"
          + "?token=${URI_TOKEN:default-token}&region=us-west-2";

  private static final List<String> LITERAL_SECRETS = List.of(
      "stream-password", "jaas-password", "stream-access-key", "decoder-api-key", "uri-user", "uri-password",
      "uri-token", "uri-signature", "batch-access-key", "batch-secret-key", "batch-credential",
      "reader-password", "tier-account-key", "connection-account-key", "custom-password", "task-auth-token",
      "task-secret-key", "schema-user", "schema-password", "dynamic-azure-key", "first-user", "first-pass",
      "second-user", "second-pass", "first-query", "second-query", "endpoint-account-key");

  @Test
  public void testRedactsCredentialsAcrossTableConfigSurfaces() {
    TableConfig stored = tableConfigWithCredentials();

    TableConfig redacted = TableConfigRedactionUtils.redact(stored);
    String redactedJson = redacted.toJsonString();

    for (String secret : LITERAL_SECRETS) {
      assertFalse(redactedJson.contains(secret), "Literal credential was not redacted: " + secret);
    }
    Map<String, String> stream = redacted.getIndexingConfig().getStreamConfigs();
    assertEquals(stream.get(STREAM_PASSWORD_KEY), MARKER);
    assertEquals(stream.get(STREAM_PLACEHOLDER_KEY), PLACEHOLDER);
    assertEquals(stream.get("stream.kafka.topic.name"), "events-topic");
    assertEquals(stream.get("stream.kafka.decoder.class.name"), "example.Decoder");
    assertEquals(stream.get("stream.kinesis.credentialsProvider"), "example.CredentialsProvider");

    String jaas = stream.get(JAAS_KEY);
    assertTrue(jaas.contains("username=\"jaas-user\""), jaas);
    assertTrue(jaas.contains("password=\"" + MARKER + "\""), jaas);
    assertTrue(jaas.contains("token=\"${JAAS_TOKEN}\""), jaas);
    assertTrue(jaas.contains("useKeyTab=true"), jaas);

    String uri = stream.get(URI_KEY);
    assertTrue(uri.contains("https://" + MARKER + ":" + MARKER + "@registry.example.test/schemas"), uri);
    assertTrue(uri.contains("access_token=" + MARKER), uri);
    assertTrue(uri.contains("X-Amz-Signature=" + MARKER), uri);
    assertTrue(uri.contains("region=us-west-2"), uri);
    assertEquals(stream.get("stream.kafka.placeholder.uri"), PLACEHOLDER_URI);
    assertEquals(stream.get("stream.kafka.decoder.prop.schema.registry.basic.auth.user.info"), MARKER);
    String uriList = stream.get("stream.kafka.credential.urls");
    assertTrue(uriList.contains("https://" + MARKER + ":" + MARKER + "@one.example.test"), uriList);
    assertTrue(uriList.contains("https://" + MARKER + ":" + MARKER + "@two.example.test"), uriList);
    assertTrue(uriList.contains("password=" + MARKER + "&region=west"), uriList);
    assertTrue(uriList.contains("token=" + MARKER + "&region=east"), uriList);
    String unquotedJaas = stream.get("stream.kafka.other.jaas.config");
    assertFalse(unquotedJaas.contains("p@ss"), unquotedJaas);
    assertFalse(unquotedJaas.contains("&word"), unquotedJaas);

    Map<String, String> batch = redacted.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps().get(0);
    assertEquals(batch.get("input.fs.prop.accessKey"), MARKER);
    assertEquals(batch.get("input.fs.prop.secretKey"), MARKER);
    assertEquals(batch.get("recordReader.prop.password"), MARKER);
    assertEquals(batch.get("recordReader.className"), "example.Reader");
    assertTrue(batch.get("input.dir.uri").contains("X-Amz-Credential=" + MARKER));
    assertEquals(batch.get("input.fs.prop.fs.azure.account.key.account.dfs.core.windows.net"), MARKER);

    Map<String, String> tier = redacted.getTierConfigsList().get(0).getTierBackendProperties();
    assertEquals(tier.get("azure.storage.accountKey"), MARKER);
    assertEquals(tier.get("endpoint"), "https://storage.example.test/container?region=west");
    assertEquals(tier.get("connection.string"),
        "DefaultEndpointsProtocol=https;AccountName=storage-user;AccountKey=" + MARKER + ";EndpointSuffix=core.test");
    assertEquals(tier.get("uri.connection.string"),
        "Endpoint=https://storage.example.test/container;AccountKey=" + MARKER + ";Region=west");
    assertEquals(redacted.getCustomConfig().getCustomConfigs().get("provider.custom.password"), MARKER);
    assertEquals(redacted.getCustomConfig().getCustomConfigs().get("placeholder.secretKey"),
        "${CUSTOM_SECRET_KEY:default-key}");
    assertEquals(redacted.getCustomConfig().getCustomConfigs().get("serviceAccountJson"),
        "${SERVICE_ACCOUNT_JSON:{\"type\":\"service_account\",\"message\":\"use } literally\"}}");
    assertEquals(redacted.getTaskConfig().getConfigsForTaskType("SegmentGenerationAndPushTask").get("authToken"),
        MARKER);

    assertEquals(stored.getIndexingConfig().getStreamConfigs().get(STREAM_PASSWORD_KEY), "stream-password");
    assertEquals(stored.getIndexingConfig().getStreamConfigs().get(URI_KEY), URI);
    assertNotSame(redacted, stored);
    assertNotSame(redacted.getIndexingConfig().getStreamConfigs(), stored.getIndexingConfig().getStreamConfigs());
  }

  @Test
  public void testRestoreRetainsCredentialsAndAppliesNonSensitiveEdit() {
    TableConfig stored = tableConfigWithCredentials();
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    submitted.getCustomConfig().getCustomConfigs().put(CLIENT_ID_KEY, "edited-client");

    TableConfig restored = TableConfigRedactionUtils.restoreRedactedValues(submitted, stored);

    assertEquals(restored.getIndexingConfig().getStreamConfigs().get(STREAM_PASSWORD_KEY), "stream-password");
    assertEquals(restored.getIndexingConfig().getStreamConfigs().get(JAAS_KEY),
        stored.getIndexingConfig().getStreamConfigs().get(JAAS_KEY));
    assertEquals(restored.getIndexingConfig().getStreamConfigs().get(URI_KEY), URI);
    assertEquals(restored.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps().get(0)
        .get("input.fs.prop.secretKey"), "batch-secret-key");
    assertEquals(restored.getCustomConfig().getCustomConfigs().get(CLIENT_ID_KEY), "edited-client");
    assertEquals(stored.getCustomConfig().getCustomConfigs().get(CLIENT_ID_KEY), "original-client");
    assertNotSame(restored, submitted);
    assertNotSame(restored.getCustomConfig().getCustomConfigs(), submitted.getCustomConfig().getCustomConfigs());
  }

  @Test
  public void testRestoreUnchangedDuplicateArrayElementsByPosition() {
    Map<String, String> first = new LinkedHashMap<>();
    first.put("streamType", "kafka");
    first.put("stream.kafka.topic.name", "same-topic");
    first.put("password", "first-password");
    Map<String, String> second = new LinkedHashMap<>();
    second.put("streamType", "kafka");
    second.put("stream.kafka.topic.name", "same-topic");
    second.put("password", "second-password");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(first, second)));
    TableConfig stored = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("duplicateStreams_REALTIME")
        .setIngestionConfig(ingestionConfig)
        .build();

    TableConfig restored = TableConfigRedactionUtils.restoreRedactedValues(
        TableConfigRedactionUtils.redact(stored), stored);

    List<Map<String, String>> streams = restored.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps();
    assertEquals(streams.get(0).get("password"), "first-password");
    assertEquals(streams.get(1).get("password"), "second-password");
  }

  @Test
  public void testRestoreAcceptsIntentionalLiteralCredentialReplacement() {
    TableConfig stored = tableConfigWithCredentials();
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    submitted.getIndexingConfig().getStreamConfigs().put(STREAM_PASSWORD_KEY, "replacement-password");

    TableConfig restored = TableConfigRedactionUtils.restoreRedactedValues(submitted, stored);

    assertEquals(restored.getIndexingConfig().getStreamConfigs().get(STREAM_PASSWORD_KEY), "replacement-password");
    assertEquals(stored.getIndexingConfig().getStreamConfigs().get(STREAM_PASSWORD_KEY), "stream-password");
  }

  @Test
  public void testRestoreRejectsMarkersThatCannotBeSafelyMerged() {
    TableConfig stored = tableConfigWithCredentials();
    TableConfig editedUri = TableConfigRedactionUtils.redact(stored);
    editedUri.getIndexingConfig().getStreamConfigs().compute(URI_KEY,
        (key, value) -> value.replace("registry.example.test", "other.example.test"));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(editedUri, stored));

    TableConfig missingStoredCredential = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("missingStored_OFFLINE")
        .setCustomConfig(new TableCustomConfig(new HashMap<>(Map.of("new.password", MARKER))))
        .build();
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(missingStoredCredential, null));
  }

  @Test
  public void testRestoreStructuredCredentialsWhileApplyingBenignEdits() {
    TableConfig stored = tableConfigWithCredentials();
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    Map<String, String> stream = submitted.getIndexingConfig().getStreamConfigs();
    stream.compute(URI_KEY, (key, value) -> value.replace("region=us-west-2", "region=us-east-1"));
    stream.compute(JAAS_KEY, (key, value) -> value.replace("useKeyTab=true", "useKeyTab=false"));
    submitted.getTierConfigsList().get(0).getTierBackendProperties().compute("connection.string",
        (key, value) -> value.replace("EndpointSuffix=core.test", "EndpointSuffix=edited.test"));

    TableConfig restored = TableConfigRedactionUtils.restoreRedactedValues(submitted, stored);

    String uri = restored.getIndexingConfig().getStreamConfigs().get(URI_KEY);
    assertTrue(uri.contains("uri-user:uri-password"), uri);
    assertTrue(uri.contains("access_token=uri-token"), uri);
    assertTrue(uri.contains("region=us-east-1"), uri);
    String jaas = restored.getIndexingConfig().getStreamConfigs().get(JAAS_KEY);
    assertTrue(jaas.contains("password=\"jaas-password\""), jaas);
    assertTrue(jaas.contains("useKeyTab=false"), jaas);
    String connection = restored.getTierConfigsList().get(0).getTierBackendProperties().get("connection.string");
    assertTrue(connection.contains("AccountKey=connection-account-key"), connection);
    assertTrue(connection.contains("EndpointSuffix=edited.test"), connection);
  }

  @Test
  public void testRestoreMatchesReorderedArrayElementsWithoutMovingCredentials() {
    Map<String, String> first = new LinkedHashMap<>();
    first.put("streamType", "kafka");
    first.put("stream.kafka.topic.name", "first-topic");
    first.put("stream.kafka.consumer.prop.password", "first-password");
    Map<String, String> second = new LinkedHashMap<>();
    second.put("streamType", "kafka");
    second.put("stream.kafka.topic.name", "second-topic");
    second.put("stream.kafka.consumer.prop.password", "second-password");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(first, second)));
    TableConfig stored = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("twoStreams_REALTIME")
        .setIngestionConfig(ingestionConfig)
        .build();
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    List<Map<String, String>> submittedStreams = submitted.getIngestionConfig().getStreamIngestionConfig()
        .getStreamConfigMaps();
    Map<String, String> firstSubmitted = submittedStreams.remove(0);
    submittedStreams.add(firstSubmitted);

    TableConfig restored = TableConfigRedactionUtils.restoreRedactedValues(submitted, stored);

    List<Map<String, String>> restoredStreams = restored.getIngestionConfig().getStreamIngestionConfig()
        .getStreamConfigMaps();
    assertEquals(restoredStreams.get(0).get("stream.kafka.topic.name"), "second-topic");
    assertEquals(restoredStreams.get(0).get("stream.kafka.consumer.prop.password"), "second-password");
    assertEquals(restoredStreams.get(1).get("stream.kafka.topic.name"), "first-topic");
    assertEquals(restoredStreams.get(1).get("stream.kafka.consumer.prop.password"), "first-password");

    TableConfig editedAndReordered = TableConfigRedactionUtils.redact(stored);
    List<Map<String, String>> editedStreams = editedAndReordered.getIngestionConfig().getStreamIngestionConfig()
        .getStreamConfigMaps();
    Map<String, String> editedFirst = editedStreams.remove(0);
    editedStreams.add(editedFirst);
    editedStreams.get(0).put("stream.kafka.topic.name", "edited-second-topic");
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(editedAndReordered, stored));
  }

  @Test
  public void testRestoreDoesNotUsePositionForDuplicateArrayIdentities() {
    Map<String, String> first = new LinkedHashMap<>();
    first.put("streamType", "kafka");
    first.put("stream.kafka.topic.name", "shared-topic");
    first.put("plugin.tenant", "first-tenant");
    first.put("password", "first-password");
    Map<String, String> second = new LinkedHashMap<>();
    second.put("streamType", "kafka");
    second.put("stream.kafka.topic.name", "shared-topic");
    second.put("plugin.tenant", "second-tenant");
    second.put("password", "second-password");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(first, second)));
    TableConfig stored = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("duplicateIdentity_REALTIME")
        .setIngestionConfig(ingestionConfig)
        .build();
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    List<Map<String, String>> streams = submitted.getIngestionConfig().getStreamIngestionConfig()
        .getStreamConfigMaps();
    streams.add(streams.remove(0));

    TableConfig restored = TableConfigRedactionUtils.restoreRedactedValues(submitted, stored);

    List<Map<String, String>> restoredStreams = restored.getIngestionConfig().getStreamIngestionConfig()
        .getStreamConfigMaps();
    assertEquals(restoredStreams.get(0).get("plugin.tenant"), "second-tenant");
    assertEquals(restoredStreams.get(0).get("password"), "second-password");
    assertEquals(restoredStreams.get(1).get("plugin.tenant"), "first-tenant");
    assertEquals(restoredStreams.get(1).get("password"), "first-password");
  }

  @Test
  public void testRestoreSamePositionMultiElementArrayWithBenignEdit() {
    Map<String, String> first = new LinkedHashMap<>();
    first.put("streamType", "kafka");
    first.put("stream.kafka.topic.name", "first-topic");
    first.put("stream.kafka.decoder.class.name", "FirstDecoder");
    first.put("stream.kafka.consumer.prop.fetch.min.bytes", "1");
    first.put("password", "first-password");
    Map<String, String> second = new LinkedHashMap<>();
    second.put("streamType", "kafka");
    second.put("stream.kafka.topic.name", "second-topic");
    second.put("stream.kafka.decoder.class.name", "SecondDecoder");
    second.put("stream.kafka.consumer.prop.fetch.min.bytes", "1");
    second.put("password", "second-password");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(first, second)));
    TableConfig stored = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("editedStreams_REALTIME")
        .setIngestionConfig(ingestionConfig)
        .build();
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    submitted.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps().get(1)
        .put("stream.kafka.consumer.prop.fetch.min.bytes", "2");

    TableConfig restored = TableConfigRedactionUtils.restoreRedactedValues(submitted, stored);

    List<Map<String, String>> streams = restored.getIngestionConfig().getStreamIngestionConfig()
        .getStreamConfigMaps();
    assertEquals(streams.get(0).get("password"), "first-password");
    assertEquals(streams.get(1).get("password"), "second-password");
    assertEquals(streams.get(1).get("stream.kafka.consumer.prop.fetch.min.bytes"), "2");
  }

  @Test
  public void testExistingCredentialEqualToMarkerStillRoundTrips() {
    TableConfig stored = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("markerCredential_OFFLINE")
        .setCustomConfig(new TableCustomConfig(new HashMap<>(Map.of("provider.password", MARKER))))
        .build();

    TableConfig restored = TableConfigRedactionUtils.restoreRedactedValues(
        TableConfigRedactionUtils.redact(stored), stored);

    assertEquals(restored.getCustomConfig().getCustomConfigs().get("provider.password"), MARKER);
  }

  @Test
  public void testRestoreAllowsMarkerInBenignValue() {
    TableConfig submitted = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("benignMarker_OFFLINE")
        .setCustomConfig(new TableCustomConfig(new HashMap<>(Map.of("display.value", MARKER))))
        .build();

    TableConfig restored = TableConfigRedactionUtils.restoreRedactedValues(submitted, null);

    assertEquals(restored.getCustomConfig().getCustomConfigs().get("display.value"), MARKER);
  }

  @Test
  public void testMixedPlaceholdersDoNotHideLiteralUriCredentials() {
    assertTrue(TableConfigRedactionUtils.isUnresolvedPlaceholder("${PASSWORD}"));
    assertTrue(TableConfigRedactionUtils.isUnresolvedPlaceholder("${PASSWORD:default-value}"));
    assertTrue(TableConfigRedactionUtils.isUnresolvedPlaceholder(
        "${SERVICE_ACCOUNT_JSON:{\"message\":\"use } literally\"}}"));
    assertFalse(TableConfigRedactionUtils.isUnresolvedPlaceholder("${PASSWORD}${TOKEN}"));
    assertFalse(TableConfigRedactionUtils.isUnresolvedPlaceholder(
        "${IGNORED} https://user:password@host.example/path}"));

    Map<String, String> custom = new HashMap<>();
    custom.put("mixed.userinfo", "https://${URI_USER}:literal-password@host.example/path");
    custom.put("mixed.query", "https://host.example/path?region=${REGION}&password=literal-query-secret");
    custom.put("placeholder.marker", "${PASSWORD:*****}");
    custom.put("assignment.placeholder", "LoginModule required password=\"${PASSWORD:*****}\";");
    TableConfig stored = tableWithCustomConfigs(custom);

    TableConfig redacted = TableConfigRedactionUtils.redact(stored);

    assertEquals(redacted.getCustomConfig().getCustomConfigs().get("mixed.userinfo"),
        "https://${URI_USER}:" + MARKER + "@host.example/path");
    assertEquals(redacted.getCustomConfig().getCustomConfigs().get("mixed.query"),
        "https://host.example/path?region=${REGION}&password=" + MARKER);
    assertEquals(redacted.getCustomConfig().getCustomConfigs().get("placeholder.marker"), "${PASSWORD:*****}");
    assertEquals(redacted.getCustomConfig().getCustomConfigs().get("assignment.placeholder"),
        "LoginModule required password=\"${PASSWORD:*****}\";");
    assertEquals(TableConfigRedactionUtils.restoreRedactedValues(redacted, stored).toJsonNode(), stored.toJsonNode());
  }

  @Test
  public void testRedactsEncodedAndNestedUriCredentialsAndKnownPrivateKeys() {
    Map<String, String> custom = new HashMap<>();
    custom.put("encoded.query", "https://host.example/path?access%5Ftoken=encoded-secret&region=west");
    custom.put("oauth.assertion", "https://idp.example/token?client_assertion=literal-client-assertion&region=west");
    custom.put("nested.uri", "https://gateway.example/path?target=https://user:pass@store.example/bucket");
    custom.put("encoded.nested.uri", "https://gateway.example/path?target="
        + "https%3A%2F%2Fencoded-user%3Aencoded-pass%40store.example%2Fbucket");
    custom.put("connection.signature", "SharedAccessSignature=sv=1&sig=shared-signature");
    custom.put("connection.whitespace",
        "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=abc def==;EndpointSuffix=core.windows.net");
    custom.put("standalone.parameters", "region=west&password=standalone-secret");
    custom.put("fs.azure.sas.container.account.blob.core.windows.net", "sp=r&sv=1&sig=literal-sas");
    custom.put("privateKeyPassphrase", "literal-passphrase");
    custom.put("provider.urls", "https://u1:p1@one.example|https://u2:p2@two.example");
    custom.put("proxy.uri", "https://proxy.example/fetch/https://nested-user:nested-pass@store.example");
    custom.put("network.path", "//relative-user:relative-pass@store.example/path");
    custom.put("semicolon.userinfo", "https://semicolon-user:p;foo=bar@store.example/path");
    custom.put("comma.userinfo", "https://comma-user:p,foo=bar@store.example/path");
    custom.put("oauth.fragment",
        "https://callback.example/path#access_token=fragment-secret&state=opaque");
    custom.put("placeholder.fragment",
        "https://callback.example/path#access_token=${FRAGMENT_TOKEN}&state=opaque");
    custom.put("plain.fragment", "https://callback.example/path#section-two");
    custom.put("auth", "Bearer literal-auth-token");
    custom.put("comma.parameters", "username=alice,password=comma-secret");
    custom.put("stream.kafka.consumer.prop.ssl.key.pem",
        "-----BEGIN PRIVATE KEY-----\npem-private-material\n-----END PRIVATE KEY-----");
    custom.put("stream.kafka.consumer.prop.ssl.keystore.key", "keystore-private-key");
    TableConfig stored = tableWithCustomConfigs(custom);

    TableConfig redacted = TableConfigRedactionUtils.redact(stored);
    String json = redacted.toJsonString();

    for (String secret : List.of("encoded-secret", "literal-client-assertion", "encoded-user", "encoded-pass",
        "shared-signature", "abc def==", "standalone-secret", "literal-sas", "literal-passphrase", "u1:", ":p1@", "u2:",
        ":p2@",
        "nested-user", "nested-pass", "relative-user", "relative-pass", "semicolon-user", "comma-user",
        "p;foo=bar", "p,foo=bar", "literal-auth-token", "comma-secret",
        "fragment-secret", "pem-private-material",
        "keystore-private-key")) {
      assertFalse(json.contains(secret), secret);
    }
    assertTrue(json.contains("region=west"), json);
    assertTrue(json.contains("client_assertion=" + MARKER + "&region=west"), json);
    assertTrue(json.contains("#access_token=" + MARKER + "&state=opaque"), json);
    assertTrue(json.contains("#access_token=${FRAGMENT_TOKEN}&state=opaque"), json);
    assertTrue(json.contains("#section-two"), json);
    assertTrue(json.contains("target=https://" + MARKER + ":" + MARKER + "@store.example/bucket"), json);
    assertEquals(TableConfigRedactionUtils.restoreRedactedValues(redacted, stored).toJsonNode(), stored.toJsonNode());
  }

  @Test
  public void testRestoreRejectsChangedStructuredSecurityIdentity() {
    Map<String, String> custom = new HashMap<>();
    custom.put("jaas", "TrustedLoginModule required password=trusted-password;");
    custom.put("connection", "Endpoint=https://trusted.example;Password=connection-password");
    custom.put("client", "clientId=trusted-client clientSecret=client-secret");
    custom.put("servicebus",
        "Endpoint=sb://trusted.example/;SharedAccessKeyName=trusted-policy;SharedAccessKey=shared-secret");
    custom.put("aws", "AccessKeyId=AKIAOLD;SecretAccessKey=aws-secret");
    custom.put("uri", "https://trusted-user:uri-password@trusted.example/path?client_id=trusted&token=query-token");
    custom.put("placeholder.uri", "https://" + "$" + "{URI_USER}:placeholder-uri-secret@trusted.example/path");
    custom.put("encoded.uri",
        "https://trusted.example/path?client%5Fid=trusted&client_secret=encoded-client-secret");
    custom.put("aws.uri", "https://s3.example/b?AWSAccessKeyId=OLD&Signature=old-signature");
    custom.put("fragment.uri", "https://trusted.example/callback#access_token=fragment-secret&state=opaque");
    custom.put("provider.assignment", "providerClass=TrustedProvider;password=provider-secret");
    custom.put("prefixed.assignment", "db.username=trusted;db.password=database-secret");
    TableConfig stored = tableWithCustomConfigs(custom);
    TableConfig changedModule = TableConfigRedactionUtils.redact(stored);
    changedModule.getCustomConfig().getCustomConfigs().compute("jaas",
        (key, value) -> value.replace("TrustedLoginModule", "AttackerLoginModule"));
    TableConfig changedEndpoint = TableConfigRedactionUtils.redact(stored);
    changedEndpoint.getCustomConfig().getCustomConfigs().compute("connection",
        (key, value) -> value.replace("trusted.example", "attacker.example"));
    TableConfig changedClient = TableConfigRedactionUtils.redact(stored);
    changedClient.getCustomConfig().getCustomConfigs().compute("client",
        (key, value) -> value.replace("trusted-client", "attacker-client"));
    TableConfig changedPolicy = TableConfigRedactionUtils.redact(stored);
    changedPolicy.getCustomConfig().getCustomConfigs().compute("servicebus",
        (key, value) -> value.replace("trusted-policy", "attacker-policy"));
    TableConfig changedAccessKey = TableConfigRedactionUtils.redact(stored);
    changedAccessKey.getCustomConfig().getCustomConfigs().compute("aws",
        (key, value) -> value.replace(MARKER + ";SecretAccessKey", "AKIANEW;SecretAccessKey"));
    TableConfig changedUriUser = TableConfigRedactionUtils.redact(stored);
    changedUriUser.getCustomConfig().getCustomConfigs().compute("uri",
        (key, value) -> value.replace(MARKER + ":", "attacker-user:"));
    TableConfig changedUriQueryIdentity = TableConfigRedactionUtils.redact(stored);
    changedUriQueryIdentity.getCustomConfig().getCustomConfigs().compute("uri",
        (key, value) -> value.replace("client_id=trusted", "client_id=attacker"));
    TableConfig changedPlaceholderUser = TableConfigRedactionUtils.redact(stored);
    changedPlaceholderUser.getCustomConfig().getCustomConfigs().compute("placeholder.uri",
        (key, value) -> value.replace("$" + "{URI_USER}", "$" + "{OTHER_USER}"));
    TableConfig changedEncodedClient = TableConfigRedactionUtils.redact(stored);
    changedEncodedClient.getCustomConfig().getCustomConfigs().compute("encoded.uri",
        (key, value) -> value.replace("client%5Fid=trusted", "client%5Fid=attacker"));
    TableConfig changedAwsAccessKey = TableConfigRedactionUtils.redact(stored);
    changedAwsAccessKey.getCustomConfig().getCustomConfigs().compute("aws.uri",
        (key, value) -> value.replace("AWSAccessKeyId=" + MARKER, "AWSAccessKeyId=NEW"));
    TableConfig changedFragmentHost = TableConfigRedactionUtils.redact(stored);
    changedFragmentHost.getCustomConfig().getCustomConfigs().compute("fragment.uri",
        (key, value) -> value.replace("trusted.example", "attacker.example"));
    TableConfig changedProviderClass = TableConfigRedactionUtils.redact(stored);
    changedProviderClass.getCustomConfig().getCustomConfigs().compute("provider.assignment",
        (key, value) -> value.replace("TrustedProvider", "AttackerProvider"));
    TableConfig changedPrefixedUser = TableConfigRedactionUtils.redact(stored);
    changedPrefixedUser.getCustomConfig().getCustomConfigs().compute("prefixed.assignment",
        (key, value) -> value.replace("db.username=trusted", "db.username=attacker"));

    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedModule, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedEndpoint, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedClient, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedPolicy, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedAccessKey, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedUriUser, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedUriQueryIdentity, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedPlaceholderUser, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedEncodedClient, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedAwsAccessKey, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedFragmentHost, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedProviderClass, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedPrefixedUser, stored));
  }

  @Test
  public void testRestoreRejectsChangedSingletonArrayIdentity() {
    Map<String, String> stream = new LinkedHashMap<>();
    stream.put("streamType", "kafka");
    stream.put("stream.kafka.topic.name", "trusted-topic");
    stream.put("password", "trusted-password");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(stream)));
    TableConfig stored = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("singleStream_REALTIME")
        .setIngestionConfig(ingestionConfig)
        .build();
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    submitted.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps().get(0)
        .put("stream.kafka.topic.name", "attacker-topic");

    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(submitted, stored));
  }

  @Test
  public void testRedactsAndRestoresJsonEncodedProviderOptions() {
    String options = "{\"username\":\"alice\",\"password\":\"json-secret\",\"region\":\"west\","
        + "\"nested\":{\"endpoint\":\"https://json-user:json-pass@store.example/path?token=json-token\"}}";
    TableConfig stored = tableWithCustomConfigs(new HashMap<>(Map.of("provider.options", options)));

    TableConfig redacted = TableConfigRedactionUtils.redact(stored);
    String redactedOptions = redacted.getCustomConfig().getCustomConfigs().get("provider.options");

    for (String secret : List.of("json-secret", "json-user", "json-pass", "json-token")) {
      assertFalse(redactedOptions.contains(secret), redactedOptions);
    }
    assertTrue(redactedOptions.contains("\"username\":\"alice\""), redactedOptions);
    assertTrue(redactedOptions.contains("\"region\":\"west\""), redactedOptions);
    redacted.getCustomConfig().getCustomConfigs().put("provider.options",
        redactedOptions.replace("\"region\":\"west\"", "\"region\":\"east\""));

    String restored = TableConfigRedactionUtils.restoreRedactedValues(redacted, stored)
        .getCustomConfig().getCustomConfigs().get("provider.options");
    assertTrue(restored.contains("\"password\":\"json-secret\""), restored);
    assertTrue(restored.contains("https://json-user:json-pass@store.example/path?token=json-token"), restored);
    assertTrue(restored.contains("\"region\":\"east\""), restored);
  }

  @Test
  public void testRedactsNumericJsonCredentialAndRejectsJsonOwnerChange() {
    String options = "{\"clientId\":\"trusted\",\"clientSecret\":\"json-secret\","
        + "\"password\":123456,\"providerClass\":\"TrustedProvider\"}";
    TableConfig stored = tableWithCustomConfigs(new HashMap<>(Map.of("provider.options", options)));
    TableConfig redacted = TableConfigRedactionUtils.redact(stored);
    String redactedOptions = redacted.getCustomConfig().getCustomConfigs().get("provider.options");

    assertFalse(redactedOptions.contains("123456"), redactedOptions);
    assertFalse(redactedOptions.contains("json-secret"), redactedOptions);
    TableConfig changedClient = TableConfigRedactionUtils.redact(stored);
    changedClient.getCustomConfig().getCustomConfigs().compute("provider.options",
        (key, value) -> value.replace("\"clientId\":\"trusted\"", "\"clientId\":\"attacker\""));
    TableConfig changedProvider = TableConfigRedactionUtils.redact(stored);
    changedProvider.getCustomConfig().getCustomConfigs().compute("provider.options",
        (key, value) -> value.replace("TrustedProvider", "AttackerProvider"));

    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedClient, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedProvider, stored));
    assertEquals(TableConfigRedactionUtils.restoreRedactedValues(redacted, stored).toJsonNode(), stored.toJsonNode());
  }

  @Test
  public void testRestoreRejectsNestedJsonProviderReassociation() {
    String options = "{\"provider\":{\"providerClass\":\"TrustedProvider\"},"
        + "\"options\":{\"password\":\"json-secret\"}}";
    TableConfig stored = tableWithCustomConfigs(new HashMap<>(Map.of("provider.options", options)));
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    submitted.getCustomConfig().getCustomConfigs().compute("provider.options",
        (key, value) -> value.replace("TrustedProvider", "AttackerProvider"));

    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(submitted, stored));
  }

  @Test
  public void testRestoreRejectsChangedAccessKeyForMaskedSecretKey() {
    Map<String, String> batch = new LinkedHashMap<>();
    batch.put("input.fs.prop.accessKey", "INPUT_ID");
    batch.put("input.fs.prop.secretKey", "INPUT_SECRET");
    batch.put("output.fs.prop.accessKey", "OUTPUT_ID");
    batch.put("output.fs.prop.secretKey", "OUTPUT_SECRET");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(List.of(batch), "APPEND", "DAILY"));
    TableConfig stored = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("credentialPair_OFFLINE")
        .setIngestionConfig(ingestionConfig)
        .build();
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    submitted.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps().get(0)
        .put("input.fs.prop.accessKey", "CHANGED_ID");

    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(submitted, stored));
  }

  @Test
  public void testStructuredTextPolicyPreservesSqlShapeAndPlaceholders() {
    String sql = "SELECT count(*) FROM source WHERE status = 'PAID' "
        + "AND endpoint = 'https://sql-user:sql-pass@store.example/path?token=sql-token&region=west' "
        + "AND configured = '" + "$" + "{ENDPOINT}'";

    String redacted = TableConfigRedactionUtils.redactStructuredText(sql);

    for (String secret : List.of("sql-user", "sql-pass", "sql-token")) {
      assertFalse(redacted.contains(secret), redacted);
    }
    assertTrue(redacted.contains("status = 'PAID'"), redacted);
    assertTrue(redacted.contains("store.example/path"), redacted);
    assertTrue(redacted.contains("region=west"), redacted);
    assertTrue(redacted.contains("'" + "$" + "{ENDPOINT}'"), redacted);
    assertTrue(redacted.endsWith("'"), redacted);
  }

  @Test
  public void testStructuredTextRedactionHandlesLongUnterminatedQuotesWithoutRecursion() {
    String backslashes = "\\".repeat(50_000);

    assertEquals(TableConfigRedactionUtils.redactStructuredText("password=\"" + backslashes),
        "password=" + MARKER);
    assertEquals(TableConfigRedactionUtils.redactStructuredText("token='" + backslashes),
        "token=" + MARKER);
  }

  @Test
  public void testStructuredTextRedactsWhitespaceInConnectionStringCredential() {
    String connectionString = "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=abc def==;"
        + "EndpointSuffix=core.windows.net";

    String redacted = TableConfigRedactionUtils.redactStructuredText(connectionString);

    assertFalse(redacted.contains("abc"), redacted);
    assertFalse(redacted.contains("def=="), redacted);
    assertTrue(redacted.contains("AccountKey=" + MARKER), redacted);
    assertTrue(redacted.contains("EndpointSuffix=core.windows.net"), redacted);
  }

  @Test
  public void testRestoreRejectsDecoderReassociation() {
    Map<String, String> stream = new LinkedHashMap<>();
    stream.put("streamType", "kafka");
    stream.put("stream.kafka.topic.name", "trusted-topic");
    stream.put("stream.kafka.decoder.class.name", "TrustedDecoder");
    stream.put("stream.kafka.consumer.prop.sasl.login.callback.handler.class", "TrustedHandler");
    stream.put("stream.kafka.consumer.prop.sasl.login.class", "TrustedLogin");
    stream.put("stream.kafka.consumer.prop.interceptor.classes", "TrustedInterceptor");
    stream.put("stream.kafka.consumer.prop.key.deserializer", "TrustedKeyDeserializer");
    stream.put("stream.kafka.consumer.prop.value.serializer", "TrustedValueSerializer");
    stream.put("stream.kafka.consumer.prop.partitioner.class", "TrustedPartitioner");
    stream.put("fs.azure.account.oauth.provider.type.account.dfs.core.windows.net", "TrustedAzureTokenProvider");
    stream.put("fs.azure.account.oauth2.client.secret.account.dfs.core.windows.net", "azure-client-secret");
    stream.put("stream.kafka.decoder.prop.password", "decoder-secret");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(stream)));
    TableConfig stored = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("decoderIdentity_REALTIME")
        .setIngestionConfig(ingestionConfig)
        .build();
    for (String selector : List.of("stream.kafka.consumer.prop.interceptor.classes",
        "stream.kafka.consumer.prop.key.deserializer", "stream.kafka.consumer.prop.value.serializer",
        "stream.kafka.consumer.prop.partitioner.class",
        "fs.azure.account.oauth.provider.type.account.dfs.core.windows.net")) {
      TableConfig submitted = TableConfigRedactionUtils.redact(stored);
      submitted.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps().get(0)
          .put(selector, "AttackerImplementation");

      assertThrows(selector, IllegalArgumentException.class,
          () -> TableConfigRedactionUtils.restoreRedactedValues(submitted, stored));
    }
  }

  @Test
  public void testRestoreRejectsLegacyDecoderReassociationButAllowsUnrelatedUriEdit() {
    Map<String, String> legacyStream = new LinkedHashMap<>();
    legacyStream.put("streamType", "kafka");
    legacyStream.put("stream.kafka.topic.name", "trusted-topic");
    legacyStream.put("stream.kafka.decoder.class.name", "TrustedDecoder");
    legacyStream.put("stream.kafka.consumer.prop.sasl.login.callback.handler.class", "TrustedHandler");
    legacyStream.put("stream.kafka.decoder.prop.password", "decoder-secret");
    TableConfig stored = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("legacyDecoderIdentity_REALTIME")
        .setStreamConfigs(legacyStream)
        .setCustomConfig(new TableCustomConfig(new HashMap<>(Map.of(
            "provider.password", "provider-secret", "callbackUrl", "https://old.example/callback"))))
        .build();
    TableConfig changedDecoder = TableConfigRedactionUtils.redact(stored);
    changedDecoder.getIndexingConfig().getStreamConfigs()
        .put("stream.kafka.consumer.prop.sasl.login.callback.handler.class", "AttackerHandler");
    TableConfig changedCallback = TableConfigRedactionUtils.redact(stored);
    changedCallback.getCustomConfig().getCustomConfigs().put("callbackUrl", "https://new.example/callback");

    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedDecoder, stored));
    TableConfig restored = TableConfigRedactionUtils.restoreRedactedValues(changedCallback, stored);
    assertEquals(restored.getCustomConfig().getCustomConfigs().get("callbackUrl"), "https://new.example/callback");
    assertEquals(restored.getCustomConfig().getCustomConfigs().get("provider.password"), "provider-secret");
  }

  @Test
  public void testRestoreRejectsLegacyEndpointAndUserReassociation() {
    Map<String, String> legacyStream = new LinkedHashMap<>();
    legacyStream.put("streamType", "kinesis");
    legacyStream.put("stream.kinesis.topic.name", "trusted-stream");
    legacyStream.put("stream.kinesis.endpoint", "https://trusted.example/path?region=west");
    legacyStream.put("stream.kinesis.accessKey", "OLD_ID");
    legacyStream.put("stream.kinesis.secretKey", "OLD_SECRET");
    legacyStream.put("stream.kinesis.username", "trusted-user");
    legacyStream.put("stream.kinesis.password", "old-password");
    TableConfig stored = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("legacyCredentialIdentity_REALTIME")
        .setStreamConfigs(legacyStream)
        .build();
    TableConfig changedEndpoint = TableConfigRedactionUtils.redact(stored);
    changedEndpoint.getIndexingConfig().getStreamConfigs().put(
        "stream.kinesis.endpoint", "https://attacker.example/path?region=west");
    TableConfig changedUsername = TableConfigRedactionUtils.redact(stored);
    changedUsername.getIndexingConfig().getStreamConfigs().put("stream.kinesis.username", "attacker-user");

    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedEndpoint, stored));
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(changedUsername, stored));
    assertEquals(TableConfigRedactionUtils.restoreRedactedValues(
        TableConfigRedactionUtils.redact(stored), stored).toJsonNode(), stored.toJsonNode());

    TableConfig aliasedUser = TableConfigRedactionUtils.redact(stored);
    aliasedUser.getIndexingConfig().getStreamConfigs().put("stream.kinesis.username", "attacker-user");
    aliasedUser.getIndexingConfig().getStreamConfigs().put("stream_kinesis_username", "trusted-user");
    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(aliasedUser, stored));
  }

  @Test
  public void testRestoreRejectsAmbiguousEditedArrayIdentity() {
    Map<String, String> first = new LinkedHashMap<>();
    first.put("streamType", "kafka");
    first.put("stream.kafka.topic.name", "first-topic");
    first.put("stream.kafka.decoder.class.name", "FirstDecoder");
    first.put("password", "first-password");
    Map<String, String> second = new LinkedHashMap<>();
    second.put("streamType", "kafka");
    second.put("stream.kafka.topic.name", "second-topic");
    second.put("stream.kafka.decoder.class.name", "SecondDecoder");
    second.put("password", "second-password");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(first, second)));
    TableConfig stored = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("ambiguousStreams_REALTIME")
        .setIngestionConfig(ingestionConfig)
        .build();
    TableConfig submitted = TableConfigRedactionUtils.redact(stored);
    List<Map<String, String>> streams = submitted.getIngestionConfig().getStreamIngestionConfig()
        .getStreamConfigMaps();
    streams.remove(0);
    streams.get(0).put("stream.kafka.topic.name", "first-topic");

    assertThrows(IllegalArgumentException.class,
        () -> TableConfigRedactionUtils.restoreRedactedValues(submitted, stored));
  }

  @Test
  public void testRedactDiagnosticPreservesContextWithoutCredentialValues() {
    TableConfig tableConfig = tableWithCustomConfigs(new HashMap<>(Map.of(
        "provider.password", "stream-password", "provider.token", "uri-token", "sasl.jaas.config",
        "example.LoginModule required password=jaas-password;")));
    String diagnostic = "Invalid replication for credentialTable_REALTIME; password=stream-password; "
        + "JAAS value contains jaas-password; URI token uri-token is invalid";

    String redacted = TableConfigRedactionUtils.redactDiagnostic(diagnostic, tableConfig);

    assertTrue(redacted.contains("Invalid replication for credentialTable_REALTIME"), redacted);
    assertFalse(redacted.contains("stream-password"), redacted);
    assertFalse(redacted.contains("jaas-password"), redacted);
    assertFalse(redacted.contains("uri-token"), redacted);

    String escapedCredential = "p\"ass\\word\nline";
    TableConfig escapedConfig = tableWithCustomConfigs(
        new HashMap<>(Map.of("provider.password", escapedCredential)));
    String escapedDiagnostic = "Invalid config " + escapedConfig.toJsonString();
    String redactedEscaped = TableConfigRedactionUtils.redactDiagnostic(escapedDiagnostic, escapedConfig);
    assertFalse(redactedEscaped.contains(escapedCredential), redactedEscaped);
    assertFalse(redactedEscaped.contains("p\\\"ass\\\\word\\nline"), redactedEscaped);

    TableConfig placeholderConfig = tableWithCustomConfigs(
        new HashMap<>(Map.of("provider.password", "${PROVIDER_PASSWORD}")));
    String placeholderDiagnostic = TableConfigRedactionUtils.redactDiagnostic(
        "Resolved provider password was resolved-literal", placeholderConfig);
    assertEquals(placeholderDiagnostic, "Invalid table config");
    assertFalse(placeholderDiagnostic.contains("resolved-literal"));
  }

  private static TableConfig tableWithCustomConfigs(Map<String, String> customConfigs) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("customConfig_OFFLINE")
        .setCustomConfig(new TableCustomConfig(customConfigs))
        .build();
  }

  private static TableConfig tableConfigWithCredentials() {
    Map<String, String> legacyStream = new LinkedHashMap<>();
    legacyStream.put("streamType", "kafka");
    legacyStream.put("stream.kafka.topic.name", "events-topic");
    legacyStream.put("stream.kafka.decoder.class.name", "example.Decoder");
    legacyStream.put("stream.kinesis.credentialsProvider", "example.CredentialsProvider");
    legacyStream.put(STREAM_PASSWORD_KEY, "stream-password");
    legacyStream.put(STREAM_PLACEHOLDER_KEY, PLACEHOLDER);
    legacyStream.put(JAAS_KEY, "example.LoginModule required username=\"jaas-user\" password=\"jaas-password\" "
        + "token=\"${JAAS_TOKEN}\" useKeyTab=true;");
    legacyStream.put("stream.kinesis.accessKey", "stream-access-key");
    legacyStream.put("stream.kafka.decoder.prop.apiKey", "decoder-api-key");
    legacyStream.put(URI_KEY, URI);
    legacyStream.put("stream.kafka.placeholder.uri", PLACEHOLDER_URI);
    legacyStream.put("stream.kafka.decoder.prop.schema.registry.basic.auth.user.info",
        "schema-user:schema-password");
    legacyStream.put("stream.kafka.credential.urls",
        "https://first-user:first-pass@one.example.test/a?password=first-query&region=west,"
            + "https://second-user:second-pass@two.example.test/b?token=second-query&region=east");
    legacyStream.put("stream.kafka.other.jaas.config", "example.LoginModule required password=p@ss&word;");

    Map<String, String> stream = new LinkedHashMap<>();
    stream.put("streamType", "kafka");
    stream.put("stream.kafka.topic.name", "secondary-topic");
    stream.put("stream.kafka.consumer.prop.password", "stream-password");
    Map<String, String> batch = new LinkedHashMap<>();
    batch.put("input.fs.prop.accessKey", "batch-access-key");
    batch.put("input.fs.prop.secretKey", "batch-secret-key");
    batch.put("input.dir.uri", "s3://bucket/path?X-Amz-Credential=batch-credential&region=us-west-2");
    batch.put("recordReader.prop.password", "reader-password");
    batch.put("recordReader.className", "example.Reader");
    batch.put("input.fs.prop.fs.azure.account.key.account.dfs.core.windows.net", "dynamic-azure-key");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(stream)));
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(List.of(batch), "APPEND", "DAILY"));

    Map<String, String> tierProperties = new LinkedHashMap<>();
    tierProperties.put("azure.storage.accountKey", "tier-account-key");
    tierProperties.put("endpoint", "https://storage.example.test/container?region=west");
    tierProperties.put("connection.string",
        "DefaultEndpointsProtocol=https;AccountName=storage-user;AccountKey=connection-account-key;"
            + "EndpointSuffix=core.test");
    tierProperties.put("uri.connection.string",
        "Endpoint=https://storage.example.test/container;AccountKey=endpoint-account-key;Region=west");
    TierConfig tierConfig = new TierConfig("TIER1", "TIME", "7d", null, "PINOT_SERVER", null, "s3",
        tierProperties);

    Map<String, String> custom = new HashMap<>();
    custom.put("provider.custom.password", "custom-password");
    custom.put(CLIENT_ID_KEY, "original-client");
    custom.put("placeholder.secretKey", "${CUSTOM_SECRET_KEY:default-key}");
    custom.put("serviceAccountJson",
        "${SERVICE_ACCOUNT_JSON:{\"type\":\"service_account\",\"message\":\"use } literally\"}}");

    Map<String, String> task = new LinkedHashMap<>();
    task.put("authToken", "task-auth-token");
    task.put("output.fs.prop.secretKey", "task-secret-key");
    task.put("provider.class", "example.Provider");

    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName("credentialTable_REALTIME")
        .setStreamConfigs(legacyStream)
        .setIngestionConfig(ingestionConfig)
        .setTierConfigList(List.of(tierConfig))
        .setCustomConfig(new TableCustomConfig(custom))
        .setTaskConfig(new TableTaskConfig(Map.of("SegmentGenerationAndPushTask", task)))
        .build();
  }
}
