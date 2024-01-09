package org.apache.pinot.tools;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.KafkaStarterUtils;


public class RealTimeSlowConsumer extends RealtimeQuickStart {
    private static final String DEFAULT_CONTROLLER_URL = "http://localhost:9000";

    @Override
    public List<String> types() {
        return Arrays.asList("REALTIME_JSON_INDEX", "REALTIME-JSON-INDEX", "STREAM_JSON_INDEX", "STREAM-JSON-INDEX");
    }

    @Override
    protected Map<String, String> getDefaultStreamTableDirectories() {
        return ImmutableMap.<String, String>builder()
                .put("githubEvents", "examples/stream/githubEvents").build();
    }

    protected void checkConnection(QuickstartRunner runner) throws Exception {
        Map<String, String> queryOptions = Collections.singletonMap("queryOptions",
                CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true");

        runner.runQuery("select 1", queryOptions);
        printStatus(Quickstart.Color.CYAN, "***** Cluster is running *****");
    }

    public void startGithubEventDataStreams(StreamDataServerStartable kafkaStarter) {
        Properties topicProperties = KafkaStarterUtils.getTopicCreationProps(2);
        topicProperties.put("retention.ms", "1000"); // Retain for 1 second only
        topicProperties.put("cleanup.policy", "delete");
        topicProperties.put("segment.bytes", "10000");
        topicProperties.put("segment.ms", "10000"); // Retain for 1 second only

        kafkaStarter.createTopic("githubEvents", topicProperties);
    }

    public void publishToDataStreams(File quickstartTmpDir) throws Exception {
        printStatus(Quickstart.Color.CYAN, "***** Starting githubEvents data stream and publishing to Kafka *****");
        publishLineSplitFileToKafka("githubEvents",
                new File(new File(quickstartTmpDir, "githubEvents"), "/rawdata/2021-07-21-few-hours.json"));
    }

    @Override
    public void execute()
            throws Exception {
        File quickstartTmpDir =
                _setCustomDataDir ? _dataDir : new File(_dataDir, String.valueOf(System.currentTimeMillis()));
        File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
        Preconditions.checkState(quickstartRunnerDir.mkdirs());
        List<QuickstartTableRequest> quickstartTableRequests = bootstrapStreamTableDirectories(quickstartTmpDir);
        final QuickstartRunner runner =
                new QuickstartRunner(quickstartTableRequests, 1, 1, 1, 1, quickstartRunnerDir, getConfigOverrides());

        startKafka();
        startGithubEventDataStreams(_kafkaStarter);

        printStatus(Quickstart.Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");
        runner.startAll();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                printStatus(Quickstart.Color.GREEN, "***** Shutting down realtime quick start *****");
                runner.stop();
                FileUtils.deleteDirectory(quickstartTmpDir);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        printStatus(Quickstart.Color.CYAN, "***** Bootstrap all tables *****");
        runner.bootstrapTable();

        this.checkConnection(runner);

        printStatus(Quickstart.Color.CYAN, "***** Publish first batch of data to kafka streams *****");
        this.publishToDataStreams(quickstartTmpDir);
        Thread.sleep(5000);

        printStatus(Quickstart.Color.CYAN, "***** Pausing consumption *****");
        HttpClient httpClient = HttpClient.getInstance();
        ControllerRequestClient client = new ControllerRequestClient(
                ControllerRequestURLBuilder.baseUrl(DEFAULT_CONTROLLER_URL), httpClient);
        client.pauseConsumption("githubEvents");

        printStatus(Quickstart.Color.CYAN, "***** Publish second batch of data to kafka streams *****");
        this.publishToDataStreams(quickstartTmpDir);
        Thread.sleep(5000);

        printStatus(Quickstart.Color.CYAN, "***** Publish third batch of data to kafka streams *****");
        this.publishToDataStreams(quickstartTmpDir);
        Thread.sleep(5000);

        client.resumeConsumption("githubEvents");

        printStatus(Quickstart.Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
    }

    public static void main(String[] args)
            throws Exception {
        List<String> arguments = new ArrayList<>();
        arguments.addAll(Arrays.asList("QuickStart", "-type", "REALTIME-JSON-INDEX"));
        arguments.addAll(Arrays.asList(args));
        PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
    }
}
