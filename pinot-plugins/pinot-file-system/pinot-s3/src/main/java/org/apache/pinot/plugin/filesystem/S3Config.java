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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.model.StorageClass;


/**
 * S3 related config
 */
public class S3Config {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3Config.class);

  private static final boolean DEFAULT_DISABLE_ACL = true;
  // From https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html, the part number must be an integer
  // between 1 and 10000, inclusive; and the min part size allowed is 5MiB, except the last one.
  private static final long MULTI_PART_UPLOAD_MIN_PART_SIZE = 5 * 1024 * 1024;
  public static final int MULTI_PART_UPLOAD_MAX_PART_NUM = 10000;

  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String REGION = "region";
  public static final String ENDPOINT = "endpoint";
  public static final String DISABLE_ACL_CONFIG_KEY = "disableAcl";

  public static final String STORAGE_CLASS = "storageClass";

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
  public static final String MIN_OBJECT_SIZE_FOR_MULTI_PART_UPLOAD = "minObjectSizeForMultiPartUpload";
  public static final String MULTI_PART_UPLOAD_PART_SIZE = "multiPartUploadPartSize";
  private static final String DEFAULT_MULTI_PART_UPLOAD_PART_SIZE = "128MB";
  public static final String DEFAULT_IAM_ROLE_BASED_ACCESS_ENABLED = "false";
  public static final String DEFAULT_SESSION_DURATION_SECONDS = "900";
  public static final String DEFAULT_ASYNC_SESSION_UPDATED_ENABLED = "true";
  public static final String DEFAULT_CROSS_REGION_ACCESS_ENABLED = "true";
  public static final String HTTP_CLIENT_CONFIG_PREFIX = "httpclient";
  public static final String HTTP_CLIENT_CONFIG_MAX_CONNECTIONS = "maxConnections";
  private static final String HTTP_CLIENT_CONFIG_SOCKET_TIMEOUT = "socketTimeout";
  private static final String HTTP_CLIENT_CONFIG_CONNECTION_TIMEOUT = "connectionTimeout";
  private static final String HTTP_CLIENT_CONFIG_CONNECTION_TIME_TO_LIVE = "connectionTimeToLive";
  private static final String HTTP_CLIENT_CONFIG_CONNECTION_ACQUISITION_TIMEOUT = "connectionAcquisitionTimeout";
  private static final String CROSS_REGION_ACCESS_ENABLED = "crossRegionAccessEnabled";
  public static final String ANONYMOUS_CREDENTIALS_PROVIDER = "anonymousCredentialsProvider";
  public static final String REQUEST_CHECKSUM_CALCULATION = "requestChecksumCalculation";
  public static final String RESPONSE_CHECKSUM_VALIDATION = "responseChecksumValidation";

  private final String _accessKey;
  private final String _secretKey;
  private final String _region;
  private final String _storageClass;
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
  private final long _minObjectSizeForMultiPartUpload;
  private final long _multiPartUploadPartSize;
  private final ApacheHttpClient.Builder _httpClientBuilder;
  private final boolean _enableCrossRegionAccess;
  private final boolean _anonymousCredentialsProvider;
  private final RequestChecksumCalculation _requestChecksumCalculationWhenRequired;
  private final ResponseChecksumValidation _responseChecksumValidationWhenRequired;

  public S3Config(PinotConfiguration pinotConfig) {
    _disableAcl = pinotConfig.getProperty(DISABLE_ACL_CONFIG_KEY, DEFAULT_DISABLE_ACL);
    _accessKey = pinotConfig.getProperty(ACCESS_KEY);
    _secretKey = pinotConfig.getProperty(SECRET_KEY);
    _region = pinotConfig.getProperty(REGION);
    _endpoint = pinotConfig.getProperty(ENDPOINT);
    _anonymousCredentialsProvider = Boolean.parseBoolean(
        pinotConfig.getProperty(ANONYMOUS_CREDENTIALS_PROVIDER, "false"));
    _requestChecksumCalculationWhenRequired = RequestChecksumCalculation.fromValue(
        pinotConfig.getProperty(REQUEST_CHECKSUM_CALCULATION, RequestChecksumCalculation.WHEN_REQUIRED.name()));
    _responseChecksumValidationWhenRequired = ResponseChecksumValidation.fromValue(
        pinotConfig.getProperty(RESPONSE_CHECKSUM_VALIDATION, ResponseChecksumValidation.WHEN_REQUIRED.name()));

    _storageClass = pinotConfig.getProperty(STORAGE_CLASS);
    if (_storageClass != null) {
      if (StorageClass.fromValue(_storageClass) == StorageClass.UNKNOWN_TO_SDK_VERSION) {
        throw new IllegalStateException(
            "unknown s3 storage class: " + _storageClass + " - Valid storage classes: " + StorageClass.knownValues());
      }
    }

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
    // Objects uploaded via putObject are limited at 5G. Setting this to 5G by default, so that smaller objects
    // continue to be uploaded by putObject methods just as before, while the larger ones are uploaded in multi parts.
    _minObjectSizeForMultiPartUpload =
        DataSizeUtils.toBytes(pinotConfig.getProperty(MIN_OBJECT_SIZE_FOR_MULTI_PART_UPLOAD, "5G"));
    _multiPartUploadPartSize = DataSizeUtils.toBytes(
        pinotConfig.getProperty(MULTI_PART_UPLOAD_PART_SIZE, DEFAULT_MULTI_PART_UPLOAD_PART_SIZE));
    Preconditions.checkArgument(_multiPartUploadPartSize > MULTI_PART_UPLOAD_MIN_PART_SIZE,
        "The part size for multipart upload must be larger than 5MB");
    if (_iamRoleBasedAccess) {
      Preconditions.checkNotNull(_roleArn, "Must provide 'roleArn' if iamRoleBasedAccess is enabled");
    }
    PinotConfiguration httpConfig = pinotConfig.subset(HTTP_CLIENT_CONFIG_PREFIX);
    _httpClientBuilder = httpConfig.isEmpty() ? null : createHttpClientBuilder(httpConfig);
    _enableCrossRegionAccess =
        Boolean.parseBoolean(pinotConfig.getProperty(CROSS_REGION_ACCESS_ENABLED, DEFAULT_CROSS_REGION_ACCESS_ENABLED));
  }

  private static ApacheHttpClient.Builder createHttpClientBuilder(PinotConfiguration config) {
    ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();
    String value = config.getProperty(HTTP_CLIENT_CONFIG_MAX_CONNECTIONS);
    if (value != null) {
      int pv = Integer.parseInt(value);
      LOGGER.debug("Set maxConnections to {} for http client builder", pv);
      httpClientBuilder.maxConnections(pv);
    }
    value = config.getProperty(HTTP_CLIENT_CONFIG_SOCKET_TIMEOUT);
    if (value != null) {
      Duration pv = parseDuration(value);
      httpClientBuilder.socketTimeout(pv);
      LOGGER.debug("Set socketTimeout to {}ms for http client builder", pv.toMillis());
    }
    value = config.getProperty(HTTP_CLIENT_CONFIG_CONNECTION_TIMEOUT);
    if (value != null) {
      Duration pv = parseDuration(value);
      httpClientBuilder.connectionTimeout(pv);
      LOGGER.debug("Set connectionTimeout to {}ms for http client builder", pv.toMillis());
    }
    value = config.getProperty(HTTP_CLIENT_CONFIG_CONNECTION_TIME_TO_LIVE);
    if (value != null) {
      Duration pv = parseDuration(value);
      httpClientBuilder.connectionTimeToLive(pv);
      LOGGER.debug("Set connectionTimeToLive to {}ms for http client builder", pv.toMillis());
    }
    value = config.getProperty(HTTP_CLIENT_CONFIG_CONNECTION_ACQUISITION_TIMEOUT);
    if (value != null) {
      Duration pv = parseDuration(value);
      httpClientBuilder.connectionAcquisitionTimeout(pv);
      LOGGER.debug("Set connectionAcquisitionTimeout to {}ms for http client builder", pv.toMillis());
    }
    return httpClientBuilder;
  }

  @VisibleForTesting
  static Duration parseDuration(String durStr) {
    try {
      // try format like '1hr20s'
      return Duration.ofMillis(TimeUtils.convertPeriodToMillis(durStr));
    } catch (Exception ignore) {
    }
    try {
      // try format like 'PT1H20S'
      return Duration.parse(durStr);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Invalid time duration '%s', for examples '1hr20s' or 'PT1H20S'", durStr), e);
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

  public long getMinObjectSizeForMultiPartUpload() {
    return _minObjectSizeForMultiPartUpload;
  }

  public long getMultiPartUploadPartSize() {
    return _multiPartUploadPartSize;
  }

  public ApacheHttpClient.Builder getHttpClientBuilder() {
    return _httpClientBuilder;
  }

  @Nullable
  public String getStorageClass() {
    return _storageClass;
  }

  public boolean isCrossRegionAccessEnabled() {
    return _enableCrossRegionAccess;
  }

  public boolean isAnonymousCredentialsProvider() {
    return _anonymousCredentialsProvider;
  }

  public RequestChecksumCalculation getRequestChecksumCalculationWhenRequired() {
    return _requestChecksumCalculationWhenRequired;
  }

  public ResponseChecksumValidation getResponseChecksumValidationWhenRequired() {
    return _responseChecksumValidationWhenRequired;
  }
}
