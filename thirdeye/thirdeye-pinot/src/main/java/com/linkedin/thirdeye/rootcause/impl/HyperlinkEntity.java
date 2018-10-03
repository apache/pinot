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

package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.util.EntityUtils;
import com.linkedin.thirdeye.rootcause.util.ParsedUrn;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class HyperlinkEntity extends Entity {
  public static final EntityType TYPE = new EntityType("http:");

  private HyperlinkEntity(String urn, double score, List<? extends Entity> related) {
    super(urn, score, related);
  }

  public String getUrl() {
    return this.getUrn();
  }

  @Override
  public HyperlinkEntity withScore(double score) {
    return new HyperlinkEntity(this.getUrn(), score, this.getRelated());
  }

  @Override
  public HyperlinkEntity withRelated(List<? extends Entity> related) {
    return new HyperlinkEntity(this.getUrn(), this.getScore(), related);
  }

  public static HyperlinkEntity fromURL(String url, double score) {
    ParsedUrn parsedUrn = EntityUtils.parseUrnString(url, TYPE);
    parsedUrn.assertPrefixOnly();
    return new HyperlinkEntity(url, score, Collections.<Entity>emptyList());
  }
}
