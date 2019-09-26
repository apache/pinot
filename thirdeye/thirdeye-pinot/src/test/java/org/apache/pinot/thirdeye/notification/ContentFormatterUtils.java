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

package org.apache.pinot.thirdeye.notification;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.commons.mail.HtmlEmail;
import org.apache.pinot.thirdeye.notification.commons.EmailEntity;


public class ContentFormatterUtils {

  public static String getHtmlContent(String htmlPath) throws IOException {
    StringBuilder htmlContent;
    try (BufferedReader br = new BufferedReader(new FileReader(htmlPath))) {
      htmlContent = new StringBuilder();
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        htmlContent.append(line).append("\n");
      }
    }

    return htmlContent.toString();
  }

  public static String getEmailHtml(EmailEntity emailEntity) throws Exception {
    HtmlEmail email = emailEntity.getContent();
    Field field = email.getClass().getDeclaredField("html");
    field.setAccessible(true);

    return field.get(email).toString();
  }
}
