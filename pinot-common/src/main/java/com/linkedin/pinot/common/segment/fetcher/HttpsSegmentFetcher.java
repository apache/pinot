/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.common.segment.fetcher;

import com.linkedin.pinot.common.utils.ClientSSLContextGenerator;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import java.util.Set;
import javax.net.ssl.SSLContext;
import org.apache.commons.configuration.Configuration;

/*
 * The following links are useful to understand some of the SSL terminology (key store, trust store, algorithms,
 * architecture, etc.)
 *
 *   https://docs.oracle.com/javase/7/docs/technotes/guides/security/crypto/CryptoSpec.html
 *   https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html
 *   https://docs.oracle.com/cd/E19509-01/820-3503/6nf1il6ek/index.html
 *
 * The 'server' in this class is not pinot-server, but the server side of the HTTPS connection.
 * SegmentFetchers are used in pinot-controller (to fetch segments from external sources),
 * and pinot-server (to fetch segment from the controller).
 *
 * Also note that this is ONE of many combinations of algorithms and types that can be used
 * for an Https client. Clearly, more configurations are possible, and can be added as needed.
 *
 * This implementation verifies the server's certificate against a Certifying Authority (CA)
 * if verification is turned on AND X509 certificate of the CA is configured.
 *
 * This implementation provides for optional configuration for key managers (where client certificate
 * is stored). If configured, the client will present its certificate to the server during TLS
 * protocol exchange. If a key manager file is not configured, then client will not present any
 * certificate to the server upon connection.
 *
 * At this time, only PKCS12 files are supported for client certificates, and it is assumed that the
 * server is presenting an X509 certificate.
 */
public class HttpsSegmentFetcher extends HttpSegmentFetcher {
  @Override
  protected void initHttpClient(Configuration configs) {
    SSLContext sslContext = new ClientSSLContextGenerator(configs.subset(CommonConstants.PREFIX_OF_SSL_SUBSET)).generate();
    _httpClient = new FileUploadDownloadClient(sslContext);
  }

  @Override
  public Set<String> getProtectedConfigKeys() {
    return ClientSSLContextGenerator.getProtectedConfigKeys();
  }

}