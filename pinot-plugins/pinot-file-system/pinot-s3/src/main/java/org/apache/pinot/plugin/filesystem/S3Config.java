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
package org.apache.pinot.plugin.filesystem;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.UUID;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * S3 related config
 */
public class S3Config {

  private static final boolean DEFAULT_DISABLE_ACL = true;

  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String REGION = "region";
  public static final String ENDPOINT = "endpoint";
  public static final String DISABLE_ACL_CONFIG_KEY = "disableAcl";

  // Encryption related configurations
  public static final String SERVER_SIDE_ENCRYPTION_CONFIG_KEY = "serverSideEncryption";
  public static final String SSE_KMS_KEY_ID_CONFIG_KEY = "ssekmsKeyId";
  public static final String SSE_KMS_ENCRYPTION_CONTEXT_CONFIG_KEY = "ssekmsEncryptionContext";

  // IAM Role related configurations
  public static final String IAM_ROLE_BASED_ACCESS_ENABLED = "iamRoleBasedAccessEnabled";
  public static final String ROLE_ARN = "roleArn";
  public static final String ROLE_SESSION_NAME = "roleSessionName";
  public static final String EXTERNAL_ID = "externalId";
  public static final String SESSION_DURATION_SECONDS = "sessionDurationSeconds";
  public static final String ASYNC_SESSION_UPDATED_ENABLED = "asyncSessionUpdateEnabled";
  public static final String DEFAULT_IAM_ROLE_BASED_ACCESS_ENABLED = "false";
  public static final String DEFAULT_SESSION_DURATION_SECONDS = "900";
  public static final String DEFAULT_ASYNC_SESSION_UPDATED_ENABLED = "true";

  private final String _accessKey;
  private final String _secretKey;
  private final String _region;
  private final boolean _disableAcl;
  private final String _endpoint;

  private final String _serverSideEncryption;
  private String _ssekmsKeyId;
  private String _ssekmsEncryptionContext;

  private boolean _iamRoleBasedAccess;
  private String _roleArn;
  private String _roleSessionName;
  private String _externalId;
  private int _sessionDurationSeconds;
  private boolean _asyncSessionUpdateEnabled;

  public S3Config(PinotConfiguration pinotConfig) {
    _disableAcl = pinotConfig.getProperty(DISABLE_ACL_CONFIG_KEY, DEFAULT_DISABLE_ACL);
    _accessKey = pinotConfig.getProperty(ACCESS_KEY);
    _secretKey = pinotConfig.getProperty(SECRET_KEY);
    _region = pinotConfig.getProperty(REGION);
    _endpoint = pinotConfig.getProperty(ENDPOINT);

    _serverSideEncryption = pinotConfig.getProperty(SERVER_SIDE_ENCRYPTION_CONFIG_KEY);
    _ssekmsKeyId = pinotConfig.getProperty(SSE_KMS_KEY_ID_CONFIG_KEY);
    _ssekmsEncryptionContext = pinotConfig.getProperty(SSE_KMS_ENCRYPTION_CONTEXT_CONFIG_KEY);

    _iamRoleBasedAccess = Boolean.parseBoolean(
        pinotConfig.getProperty(IAM_ROLE_BASED_ACCESS_ENABLED, DEFAULT_IAM_ROLE_BASED_ACCESS_ENABLED));
    _roleArn = pinotConfig.getProperty(ROLE_ARN);
    _roleSessionName =
        pinotConfig.getProperty(ROLE_SESSION_NAME, Joiner.on("-").join("pinot", "s3", UUID.randomUUID()));
    _externalId = pinotConfig.getProperty(EXTERNAL_ID);
    _sessionDurationSeconds =
        Integer.parseInt(pinotConfig.getProperty(SESSION_DURATION_SECONDS, DEFAULT_SESSION_DURATION_SECONDS));
    _asyncSessionUpdateEnabled = Boolean.parseBoolean(
        pinotConfig.getProperty(ASYNC_SESSION_UPDATED_ENABLED, DEFAULT_ASYNC_SESSION_UPDATED_ENABLED));

    if (_iamRoleBasedAccess) {
      Preconditions.checkNotNull(_roleArn, "Must provide 'roleArn' if iamRoleBasedAccess is enabled");
    }
  }

  public String getAccessKey() {
    return _accessKey;
  }

  public String getSecretKey() {
    return _secretKey;
  }

  public String getRegion() {
    return _region;
  }

  public boolean getDisableAcl() {
    return _disableAcl;
  }

  public String getEndpoint() {
    return _endpoint;
  }

  public String getServerSideEncryption() {
    return _serverSideEncryption;
  }

  public String getSseKmsKeyId() {
    return _ssekmsKeyId;
  }

  public String getSsekmsEncryptionContext() {
    return _ssekmsEncryptionContext;
  }

  public boolean isIamRoleBasedAccess() {
    return _iamRoleBasedAccess;
  }

  public String getRoleArn() {
    return _roleArn;
  }

  public String getRoleSessionName() {
    return _roleSessionName;
  }

  public String getExternalId() {
    return _externalId;
  }

  public int getSessionDurationSeconds() {
    return _sessionDurationSeconds;
  }

  public boolean isAsyncSessionUpdateEnabled() {
    return _asyncSessionUpdateEnabled;
  }
}
