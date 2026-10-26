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
package org.apache.pinot.plugin.stream.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.pinot.spi.ingestion.error.IngestionSourceCredentialMode;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorEvidence;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict;


/// The evidence the Kafka plugins attach to the exceptions they create for a source failure. These are templates:
/// [IngestionSourceErrorEvidence#attach] copies them onto each failure.
public final class KafkaSourceErrorEvidence {
  /// The broker's metadata lacks the topic, which the synthesized "Topic does not exist" doesn't say.
  public static final IngestionSourceErrorEvidence TOPIC_MISSING = new IngestionSourceErrorEvidence()
      .setTarget(IngestionSourceErrorVerdict.TARGET_BROKER)
      .setEvidence(0, IngestionSourceErrorEvidence.UNKNOWN_TOPIC_OR_PARTITION);
  /// The broker never answered in time.
  public static final IngestionSourceErrorEvidence BROKER_UNREACHABLE = new IngestionSourceErrorEvidence()
      .setTarget(IngestionSourceErrorVerdict.TARGET_BROKER)
      .setEvidence(0, IngestionSourceErrorEvidence.UNREACHABLE);

  // aws-msk-iam-auth: its SASL mechanism, its OAUTHBEARER callback handlers' package, and the JAAS options it reads
  // for an assumed role
  private static final String AWS_MSK_IAM = "AWS_MSK_IAM";
  private static final String MSK_IAM_PACKAGE = "software.amazon.msk.auth.iam.";
  private static final String MSK_ROLE_ARN = "awsRoleArn";
  private static final String MSK_ROLE_ACCESS_KEY_ID = "awsRoleAccessKeyId";
  private static final String MSK_ROLE_SECRET_ACCESS_KEY = "awsRoleSecretAccessKey";

  private KafkaSourceErrorEvidence() {
  }

  /// For an MSK IAM table, whose identity aws-msk-iam-auth signs with: the process's own AWS credential chain
  /// (AMBIENT), optionally to assume `awsRoleArn`. Static keys given for that role are a credential configured on the
  /// table (STATIC). Returns null for any other table. Only the presence of these options is recorded, and the JAAS
  /// config is never logged.
  @Nullable
  public static IngestionSourceErrorEvidence mskIam(Properties consumerProps) {
    String loginCallbackHandler = consumerProps.getProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
    if (!AWS_MSK_IAM.equals(consumerProps.getProperty(SaslConfigs.SASL_MECHANISM))
        && (loginCallbackHandler == null || !loginCallbackHandler.startsWith(MSK_IAM_PACKAGE))) {
      return null;
    }
    Map<String, ?> options = Map.of();
    String jaasConfig = consumerProps.getProperty(SaslConfigs.SASL_JAAS_CONFIG);
    if (jaasConfig != null) {
      try {
        // Kafka's own parser, as the login module receives it
        List<AppConfigurationEntry> entries =
            JaasContext.loadClientContext(Map.of(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasConfig)))
                .configurationEntries();
        options = entries.get(0).getOptions();
      } catch (RuntimeException e) {
        // Not parsable here, e.g. a config provider reference: assume no options, so the process's own chain. The
        // message can quote the config, so it is dropped.
      }
    }
    boolean roleArnConfigured = options.containsKey(MSK_ROLE_ARN);
    boolean staticRoleKeys = roleArnConfigured && options.containsKey(MSK_ROLE_ACCESS_KEY_ID)
        && options.containsKey(MSK_ROLE_SECRET_ACCESS_KEY);
    IngestionSourceCredentialMode credentialMode =
        staticRoleKeys ? IngestionSourceCredentialMode.STATIC : IngestionSourceCredentialMode.AMBIENT;
    return new IngestionSourceErrorEvidence().setCredentialMode(credentialMode).setRoleArnConfigured(roleArnConfigured);
  }
}
