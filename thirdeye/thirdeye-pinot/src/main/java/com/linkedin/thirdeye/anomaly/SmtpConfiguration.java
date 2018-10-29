/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.anomaly;

import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import org.apache.commons.collections.MapUtils;


public class SmtpConfiguration {

  public static final String SMTP_CONFIG_KEY = "smtpConfiguration";
  public static final String SMTP_HOST_KEY = "smtpHost";
  public static final String SMTP_PORT_KEY = "smtpPort";
  public static final String SMTP_USER_KEY = "smtpUser";
  public static final String SMTP_PASSWD_KEY = "smtpPassword";

  private String smtpHost;
  private int smtpPort = 25;
  private String smtpUser;
  private String smtpPassword;

  public String getSmtpHost() {
    return smtpHost;
  }

  public void setSmtpHost(String smtpHost) {
    this.smtpHost = smtpHost;
  }

  public int getSmtpPort() {
    return smtpPort;
  }

  public void setSmtpPort(int smtpPort) {
    this.smtpPort = smtpPort;
  }

  public String getSmtpUser() {
    return smtpUser;
  }

  public void setSmtpUser(String smtpUser) {
    this.smtpUser = smtpUser;
  }

  public String getSmtpPassword() {
    return smtpPassword;
  }

  public void setSmtpPassword(String smtpPassword) {
    this.smtpPassword = smtpPassword;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SmtpConfiguration)) {
      return false;
    }
    SmtpConfiguration at = (SmtpConfiguration) o;
    return Objects.equals(smtpHost, at.getSmtpHost())
        && Objects.equals(smtpPort, at.getSmtpPort())
        && Objects.equals(smtpUser, at.getSmtpUser())
        && Objects.equals(smtpPassword, at.getSmtpPassword());
  }

  @Override
  public int hashCode() {
    return Objects.hash(smtpHost, smtpPort, smtpUser, smtpPassword);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add(SMTP_HOST_KEY, smtpHost).add(SMTP_PORT_KEY, smtpPort)
        .add(SMTP_USER_KEY, smtpUser).toString();
  }

  public static SmtpConfiguration createFromProperties(Map<String,Object> smtpConfiguration) {
    SmtpConfiguration conf = new SmtpConfiguration();
    try {
      conf.setSmtpHost(MapUtils.getString(smtpConfiguration, SMTP_HOST_KEY));
      conf.setSmtpPort(MapUtils.getIntValue(smtpConfiguration, SMTP_PORT_KEY));
      conf.setSmtpUser(MapUtils.getString(smtpConfiguration, SMTP_USER_KEY));
      conf.setSmtpPassword(MapUtils.getString(smtpConfiguration, SMTP_PASSWD_KEY));
    } catch (Exception e) {
      throw new RuntimeException("Error occurred while parsing smtp configuration into object.", e);
    }
    return conf;
  }
}
