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
package com.linkedin.pinot.tools.perf;

import com.linkedin.pinot.filesystem.AzurePinotFS;
import com.linkedin.pinot.tools.AbstractBaseCommand;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.kohsuke.args4j.Option;


public class AzureFs extends AbstractBaseCommand implements Command {

  @Option(name = "-exists", required = false, metaVar = "<String>",
      usage = "Check if file exists")
  private String uriExists;

  @Option(name = "-create", required = false, metaVar = "<String>",
      usage = "Check if file exists")
  private String createFile;

  @Option(name = "-config", required = true, metaVar = "<String>",
      usage = "Check if file exists")
  private String configFile;

  @Override
  public boolean execute() throws Exception {
//    System.out.println("Fetching url...");
//    URL url;
//
//    url = new URL(uriExists);
//    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//
//    BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//
//    String inputLine;
//    while ((inputLine = br.readLine()) != null) {
//      System.out.println(inputLine);
//    }
//
//    if (true) return true;
    AzurePinotFS fs = new AzurePinotFS();
    Configuration configuration = new PropertiesConfiguration();
    configuration.addProperty("azure.account", "k2pcxdls.azuredatalakestore.net");
    configuration.addProperty("azure.authendpoint", "https://login.microsoftonline.com/588e8fae-da1b-4c8b-810f-3229806c04fe/oauth2/token");
    configuration.addProperty("azure.clientid", "757c4f11-8d69-47d4-9e18-e540c72911f0");
    configuration.addProperty("azure.clientsecret", "BEnEccsQHd/QbvQmGO6f17V0LFLEdssypAYrd4qhXCw=");

    fs.init(configuration);

    if (uriExists != null) {
      if (fs.exists(new URI(uriExists))) {
        System.out.println("URI exists: " + uriExists);
      } else {
        System.out.println("URI not found: " + uriExists);
      }
    } else if (createFile != null) {
      System.out.println("Trying to create file: " + createFile);
      fs.createFile(createFile);
    }

    // Try to copy file to local
    File tmpFile = new File(System.getProperty("java.io.tmpdir"), AzureFs.class.getSimpleName());
    System.out.println(tmpFile.getAbsolutePath());

    fs.copyToLocalFile(new URI(uriExists), tmpFile);

    System.out.println(Arrays.toString(fs.listFiles(new URI("/"))));

    System.out.println(fs.length(new URI(uriExists)));

    // copy
    URI newURI = new URI("adl:/PINOTDATA.tar.gz");
    fs.copy(new URI(uriExists), newURI);
    System.out.println("length old : " + fs.length(new URI(uriExists)));
    System.out.println("length : " + fs.length(newURI));

    System.out.println("should exist " + fs.exists(newURI));

    // delete
    fs.delete(newURI);

    System.out.println("should not exist" + fs.exists(newURI));

    // move
    fs.move(new URI(uriExists), newURI);
    System.out.println("should exist " + fs.exists(newURI));
    System.out.println("should not exist  " + fs.exists(new URI(uriExists)));

    fs.move(newURI, new URI(uriExists));
    System.out.println("should not exist " + fs.exists(newURI));
    System.out.println("should exist  " + fs.exists(new URI(uriExists)));

    return true;
  }

  @Override
  public String description() {
    return null;
  }

  @Override
  public boolean getHelp() {
    return false;
  }

  public static void main(String[] args) throws Exception {
    AzureFs azureFs = new AzureFs();
    azureFs.uriExists = "/jobForecasting_0.tar.gz";
    System.out.println(azureFs.execute());
//    PropertiesConfiguration conf = new PropertiesConfiguration("/home/mshrivas/azure/config.txt");
//    System.out.println(ConfigurationUtils.toString(conf));
  }
}
