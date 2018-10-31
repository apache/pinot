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

package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import org.apache.commons.mail.HtmlEmail;


public class EmailEntity {
  private String from;
  private DetectionAlertFilterRecipients to;
  private String subject;
  private HtmlEmail content;

  public EmailEntity() {

  }

  public EmailEntity(String from, DetectionAlertFilterRecipients to, String subject, HtmlEmail content) {
    this.from = from;
    this.to = to;
    this.subject = subject;
    this.content = content;
  }

  public String getFrom() {
    return from;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public DetectionAlertFilterRecipients getTo() {
    return to;
  }

  public void setTo(DetectionAlertFilterRecipients to) {
    this.to = to;
  }

  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public HtmlEmail getContent() {
    return content;
  }

  public void setContent(HtmlEmail content) {
    this.content = content;
  }
}
