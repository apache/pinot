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
package com.linkedin.pinot.common.segment.crypt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The MultiPartPinotCrypter takes in a multipart object and extracts the tarred segment file.
 */
public class MultipartPinotCrypter implements PinotCrypter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultipartPinotCrypter.class);

  @Override
  public void init(Configuration config) {

  }

  @Override
  public void encrypt(Object decryptedObject, File encryptedFile) {
    throw new UnsupportedOperationException("Multipart Pinot Crypter does not support encryption");
  }

  @Override
  public void decrypt(Object encryptedObject, File decryptedFile) {
    if (! (encryptedObject instanceof FormDataMultiPart)) {
      throw new RuntimeException("Expected multipart object for decryption");
    }

    FormDataMultiPart multiPart = (FormDataMultiPart) encryptedObject;
    // Read segment file or segment metadata file and directly use that information to update zk
    Map<String, List<FormDataBodyPart>> segmentMetadataMap = multiPart.getFields();
    if (!validateMultiPart(segmentMetadataMap, null)) {
      throw new RuntimeException("Invalid multi-part form for segment metadata");
    }
    FormDataBodyPart segmentMetadataBodyPart = segmentMetadataMap.values().iterator().next().get(0);
    try (InputStream inputStream = segmentMetadataBodyPart.getValueAs(InputStream.class);
        OutputStream outputStream = new FileOutputStream(decryptedFile)) {
      IOUtils.copyLarge(inputStream, outputStream);
    } catch (IOException e) {
      LOGGER.error("Could not decrypt to {}", decryptedFile.getAbsolutePath());
    }
    finally {
      multiPart.cleanup();
    }
  }

  // Validate that there is one file that is in the input.
  private boolean validateMultiPart(Map<String, List<FormDataBodyPart>> map, String segmentName) {
    boolean isGood = true;
    if (map.size() != 1) {
      LOGGER.warn("Incorrect number of multi-part elements: {} (segmentName {}). Picking one", map.size(), segmentName);
      isGood = false;
    }
    List<FormDataBodyPart> bodyParts = map.get(map.keySet().iterator().next());
    if (bodyParts.size() != 1) {
      LOGGER.warn("Incorrect number of elements in list in first part: {} (segmentName {}). Picking first one",
          bodyParts.size(), segmentName);
      isGood = false;
    }
    return isGood;
  }
}
