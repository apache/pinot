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
package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.minion.MinionContext;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.httpclient.Header;


public abstract class BaseTaskExecutor implements PinotTaskExecutor {
  protected MinionContext _minionContext;
  protected boolean _cancelled = false;

  @Override
  public void setMinionContext(@Nonnull MinionContext minionContext) {
    _minionContext = minionContext;
  }

  @Override
  public void cancel() {
    _cancelled = true;
  }

  @Override
  public int uploadSegment(String uri, final String fileName, final InputStream inputStream, final long lengthInBytes,
      FileUploadUtils.SendFileMethod httpMethod, String originalSegmentCrc) {
    List<Header> headers = new ArrayList<>();
    Header header = new Header("If-Match", originalSegmentCrc);

    // A pragma header is an implementation specific field that may have various effects along the
    // request response chain. We will tell the controller minion is sending the segment.
    Header minionHeader = new Header("Pragma", "Minion");

    headers.add(header);
    headers.add(minionHeader);

    return FileUploadUtils.sendFile(uri, fileName, inputStream, lengthInBytes, FileUploadUtils.SendFileMethod.POST, headers);
  }
}
