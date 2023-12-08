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
package org.apache.pinot.tools;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;


public class RealtimeQuickStart extends QuickStartBase {
  @Override
  public List<String> types() {
    return Arrays.asList("REALTIME", "STREAM");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "REALTIME"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    String q1 = "select count(*) from meetupRsvp limit 1";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 =
        "select group_city, sum(rsvp_count) from meetupRsvp group by group_city order by sum(rsvp_count) desc limit 10";
    printStatus(Color.YELLOW, "Top 10 cities with the most rsvp");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 = "select * from meetupRsvp order by mtime limit 10";
    printStatus(Color.YELLOW, "Show 10 most recent rsvps");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    String q4 =
        "select event_name, sum(rsvp_count) from meetupRsvp group by event_name order by sum(rsvp_count) desc limit 10";
    printStatus(Color.YELLOW, "Show top 10 rsvp'ed events");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    String q5 = "select count(*) from meetupRsvp limit 1";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q5);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Color.GREEN, "***************************************************");
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir =
        _setCustomDataDir ? _dataDir : new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");

    String clusterName = "srcds";
    setupTestContainers(clusterName);

    Preconditions.checkState(quickstartRunnerDir.mkdirs());
    List<QuickstartTableRequest> quickstartTableRequests = bootstrapStreamTableDirectories(quickstartTmpDir);
    final QuickstartRunner runner =
        new QuickstartRunner(quickstartTableRequests, 1, 1, 3, 0, quickstartRunnerDir, getConfigOverrides());

    int zkPort = new Random().nextInt(50_000);
    startKafka(zkPort);
    startAllDataStreams(_kafkaStarter, quickstartTmpDir);

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");

    runner.startAll(zkPort, clusterName);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down realtime quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    printStatus(Color.CYAN, "***** Bootstrap all tables *****");
    runner.bootstrapTable();

    printStatus(Color.CYAN, "***** Waiting for 5 seconds for a few events to get populated *****");
    Thread.sleep(5000);

    printStatus(Color.YELLOW, "***** Realtime quickstart setup complete *****");
    runSampleQueries(runner);
//
    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }

  private void setupTestContainers(String bucketName) {
    try (GenericContainer<?> s3Container = new GenericContainer<>("adobe/s3mock:2.1.14").withEnv(
        Map.of("AWS_REGION", "us-west-2", "ACCESS_KEY", "accessKey", "SECRET_KEY", "secretKey"))) {

      s3Container.withCreateContainerCmdModifier(cmd -> {
        cmd.getHostConfig().withPortBindings(new PortBinding(Ports.Binding.bindPort(50_000), new ExposedPort(9090)));
      });

      s3Container.start();

      // Get S3 endpoint URL
      String s3Endpoint = "http://" + s3Container.getHost() + ":" + s3Container.getMappedPort(9090);

      // Use Testcontainers S3 endpoint in your S3 client configuration
      S3Client s3Client =
          S3Client.builder().forcePathStyle(true).region(Region.US_WEST_2).endpointOverride(URI.create(s3Endpoint))
              .credentialsProvider(
                  StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey"))).build();

      while (true) {
        try {
          s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
          break;
        } catch (Exception e) {
          continue;
        }
      }

      System.out.println("Created bucket");
    }
  }
}
