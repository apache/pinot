package com.linkedin.thirdeye.alert.content.imageEmbed;

import java.io.IOException;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;


public interface EmailImageEmbed {
  /**
   * get the key of the image cid
   * @return image cid
   */
  String getName();
  /**
   * Embed image to email
   * @param email an email instance
   * @return cid of the embedded image
   * @throws EmailException
   */
  String embed(HtmlEmail email) throws EmailException;

  /**
   * Clean up the image from file system
   * @throws IOException
   */
  void cleanup() throws IOException;
}
