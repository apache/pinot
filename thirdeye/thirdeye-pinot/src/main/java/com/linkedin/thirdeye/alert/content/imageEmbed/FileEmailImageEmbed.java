package com.linkedin.thirdeye.alert.content.imageEmbed;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import javax.activation.URLDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;


public class FileEmailImageEmbed implements EmailImageEmbed{
  private String imagePath;
  private String name;

  public FileEmailImageEmbed(String imagePath, String name) throws FileNotFoundException{
    if (StringUtils.isBlank(imagePath)) {
      throw new FileNotFoundException("Image file path is empty.");
    }
    this.imagePath = imagePath;
    this.name = name;
  }

  /**
   * Embed image to email
   * @param email an email instance
   * @return cid of the embedded image
   * @throws EmailException
   */
  @Override
  public String embed(HtmlEmail email) throws EmailException {
    URLDataSource dataSource = new URLDataSource(getClass().getClassLoader().getResource(imagePath));
    return email.embed(dataSource, "logo");
  }

  /**
   * Clean up the image from file system
   * @throws IOException
   */
  @Override
  public void cleanup() throws IOException {
    /*
     No clean up need to be performed
     */
  }

  /**
   * get the key of the image cid
   * @return image cid
   */
  @Override
  public String getName() {
    return name;
  }
}
