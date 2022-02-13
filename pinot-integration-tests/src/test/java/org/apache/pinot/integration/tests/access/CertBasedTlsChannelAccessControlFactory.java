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
package org.apache.pinot.integration.tests.access;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;
import java.io.ByteArrayInputStream;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.access.RequesterIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CertBasedTlsChannelAccessControlFactory implements AccessControlFactory {

  @Override
  public AccessControl create() {
    return new CertBasedTlsChannelAccessControl();
  }

  static public class CertBasedTlsChannelAccessControl implements AccessControl {
    private final Logger _logger = LoggerFactory.getLogger(CertBasedTlsChannelAccessControl.class);

    private final Set<String> _aclPrincipalAllowlist = new HashSet<String>() {{
      add("CN=test-jks, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown");
      add("CN=test-p12, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown");
    }};

    @Override
    public boolean isAuthorizedChannel(ChannelHandlerContext channelHandlerContext) {
      try {
        SslHandler sslhandler = (SslHandler) channelHandlerContext.channel().pipeline().get("ssl");
        Certificate clientCert = sslhandler.engine().getSession().getPeerCertificates()[0];
        ByteArrayInputStream bais = new ByteArrayInputStream(clientCert.getEncoded());
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate x509 = (X509Certificate) cf.generateCertificate(bais);
        Principal principal = x509.getSubjectX500Principal();
        return _aclPrincipalAllowlist.contains(principal.toString());
      } catch (CertificateException | SSLPeerUnverifiedException e) {
        _logger.error("Access denied - caught exception while validating access to server, with channelHandlerContext:"
            + channelHandlerContext, e);
        return false;
      }
    }

    @Override
    public boolean hasDataAccess(RequesterIdentity requesterIdentity, String tableName) {
      return true;
    }
  }
}
