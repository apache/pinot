package org.apache.pinot.plugin.stream.kinesis;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.pinot.spi.stream.Checkpoint;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupInfo;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import static org.easymock.EasyMock.*;


@PrepareForTest(Shard.class)
public class KinesisStreamMetadataProviderTest extends PowerMockTestCase {
    private static final String STREAM_NAME = "kinesis-test";
    private static final String AWS_REGION = "us-west-2";

    private static final String SHARD_ID_0 = "0";
    private static final String SHARD_ID_1 = "1";
    private static final String SHARD_ITERATOR_0 = "0";
    private static final String SHARD_ITERATOR_1 = "1";
    public static final String CLIENT_ID = "dummy";
    public static final int TIMEOUT = 1000;

    private static Shard shard0;
    private static Shard shard1;
    private static KinesisConnectionHandler kinesisConnectionHandler;
    private KinesisStreamMetadataProvider kinesisStreamMetadataProvider;
    private static StreamConsumerFactory streamConsumerFactory;
    private static PartitionGroupConsumer partitionGroupConsumer;

    @BeforeMethod
    public void setupTest()
    {
        kinesisConnectionHandler = PowerMock.createMock(KinesisConnectionHandler.class);
        shard0 = PowerMock.createMock(Shard.class);
        shard1 = PowerMock.createMock(Shard.class);
        streamConsumerFactory = PowerMock.createMock(StreamConsumerFactory.class);
        partitionGroupConsumer = PowerMock.createNiceMock(PartitionGroupConsumer.class);
        kinesisStreamMetadataProvider = new KinesisStreamMetadataProvider(CLIENT_ID, TestUtils.getStreamConfig(), kinesisConnectionHandler, streamConsumerFactory);
    }

    @Test
    public void getPartitionsGroupInfoListTest() throws Exception {
        EasyMock.expect(kinesisConnectionHandler.getShards()).andReturn(ImmutableList.of(shard0, shard1)).anyTimes();
        EasyMock.expect(shard0.shardId()).andReturn(SHARD_ID_0).anyTimes();
        EasyMock.expect(shard1.shardId()).andReturn(SHARD_ID_1).anyTimes();
        EasyMock.expect(shard0.parentShardId()).andReturn(null).anyTimes();
        EasyMock.expect(shard1.parentShardId()).andReturn(null).anyTimes();
        EasyMock.expect(shard0.sequenceNumberRange()).andReturn(SequenceNumberRange.builder().startingSequenceNumber("1").build()).anyTimes();
        EasyMock.expect(shard1.sequenceNumberRange()).andReturn(SequenceNumberRange.builder().startingSequenceNumber("1").build()).anyTimes();

        replay(kinesisConnectionHandler, shard0, shard1);

        List<PartitionGroupInfo> result = kinesisStreamMetadataProvider.getPartitionGroupInfoList(CLIENT_ID, TestUtils.getStreamConfig(), new ArrayList<>(),
            TIMEOUT);

        Assert.assertEquals(result.size(), 2);
        Assert.assertEquals(result.get(0).getPartitionGroupId(), 0);
        Assert.assertEquals(result.get(1).getPartitionGroupId(), 1);
    }

    @Test
    public void getPartitionsGroupInfoSinglePartitionTest() throws Exception {
        List<PartitionGroupMetadata> currentPartitionGroupMeta = new ArrayList<>();

        Map<String, String> shardToSequenceMap = new HashMap<>();
        shardToSequenceMap.put("0", "1");
        KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(shardToSequenceMap);

        currentPartitionGroupMeta.add(new PartitionGroupMetadata(0, 1, kinesisCheckpoint, kinesisCheckpoint, "CONSUMING"));

        Capture<Checkpoint> checkpointArgs = newCapture(CaptureType.ALL);
        Capture<PartitionGroupMetadata> partitionGroupMetadataCapture = newCapture(CaptureType.ALL);
        Capture<Integer> intArguments = newCapture(CaptureType.ALL);
        Capture<String> stringCapture = newCapture(CaptureType.ALL);

        EasyMock.expect(kinesisConnectionHandler.getShards()).andReturn(ImmutableList.of(shard0, shard1)).anyTimes();
        EasyMock.expect(shard0.shardId()).andReturn(SHARD_ID_0).anyTimes();
        EasyMock.expect(shard1.shardId()).andReturn(SHARD_ID_1).anyTimes();
        EasyMock.expect(shard0.parentShardId()).andReturn(null).anyTimes();
        EasyMock.expect(shard1.parentShardId()).andReturn(null).anyTimes();
        EasyMock.expect(shard0.sequenceNumberRange()).andReturn(SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("1").build()).anyTimes();
        EasyMock.expect(shard1.sequenceNumberRange()).andReturn(SequenceNumberRange.builder().startingSequenceNumber("1").build()).anyTimes();
        EasyMock.expect(streamConsumerFactory.createPartitionGroupConsumer(capture(stringCapture), capture(partitionGroupMetadataCapture))).andReturn(partitionGroupConsumer).anyTimes();
        EasyMock.expect(partitionGroupConsumer.fetchMessages(capture(checkpointArgs), capture(checkpointArgs), captureInt(intArguments))).andReturn(new KinesisRecordsBatch(new ArrayList<>(), "0", true)).anyTimes();

        replay(kinesisConnectionHandler, shard0, shard1, streamConsumerFactory, partitionGroupConsumer);

        List<PartitionGroupInfo> result = kinesisStreamMetadataProvider.getPartitionGroupInfoList(CLIENT_ID, TestUtils.getStreamConfig(), currentPartitionGroupMeta,
            TIMEOUT);

        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0).getPartitionGroupId(), 1);
    }
}
