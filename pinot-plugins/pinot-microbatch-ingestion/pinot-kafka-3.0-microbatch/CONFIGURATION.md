<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Kafka Microbatch Ingestion Configuration

## Overview

The Kafka Microbatch module enables batch-oriented ingestion from Kafka topics where messages contain references to batch files (URI) or inline encoded data. This is useful for scenarios where:

- Batch files are written to object storage (S3, GCS, Azure, HDFS) and only references are sent to Kafka
- Large batches need to be grouped and sent as single Kafka messages
- External batch processing systems produce output files that need to be ingested

## Protocol Format

Messages in Kafka must follow the JSON protocol format (version 1.0):

### Type 1: File Reference (URI)
```json
{
  "version": "1.0",
  "type": "uri",
  "format": "avro",
  "uri": "s3://bucket/path/to/batch-12345.avro"
}
```

### Type 2: Inline Data
```json
{
  "version": "1.0",
  "type": "data",
  "format": "avro",
  "data": "<base64-encoded-avro-bytes>"
}
```

### Required Fields
- `version` (string): Protocol version, must be "1.0"
- `type` (string): Message type - "uri" or "data"
- `format` (string): Data format - "avro", "parquet", or "json"
- `uri` (string): File URI (required when type="uri")
- `data` (string): Base64-encoded data (required when type="data")

## Table Configuration

### Minimum Configuration

```json
{
  "tableName": "myTable",
  "tableType": "REALTIME",
  "segmentsConfig": {
    "timeColumnName": "timestamp",
    "replication": "1",
    "schemaName": "myTable"
  },
  "tableIndexConfig": {
    "loadMode": "MMAP",
    "streamConfigs": {
      "streamType": "kafka",
      "stream.kafka.topic.name": "microbatch-topic",
      "stream.kafka.broker.list": "localhost:9092",
      "stream.kafka.consumer.type": "lowlevel",
      "stream.kafka.consumer.factory.class.name":
        "org.apache.pinot.plugin.stream.microbatch.kafka30.KafkaMicroBatchConsumerFactory",
      "realtime.segment.flush.threshold.rows": "5000000",
      "realtime.segment.flush.threshold.time": "3600000"
    }
  },
  "tenants": {},
  "metadata": {}
}
```

### Key Configuration Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `stream.kafka.consumer.factory.class.name` | Yes | Must be `org.apache.pinot.plugin.stream.microbatch.kafka30.KafkaMicroBatchConsumerFactory` |
| `stream.kafka.topic.name` | Yes | Kafka topic containing microbatch protocol messages |
| `stream.kafka.broker.list` | Yes | Kafka broker addresses |

## PinotFS Configuration

For URI-based messages, configure the appropriate PinotFS implementation based on your storage system.

### S3 Configuration

**Server/Controller startup properties:**
```properties
# S3 Filesystem
pinot.fs.s3.class=org.apache.pinot.plugin.filesystem.S3PinotFS
pinot.fs.s3.region=us-west-2
pinot.fs.s3.accessKey=<access-key>
pinot.fs.s3.secretKey=<secret-key>

# Or use IAM roles (recommended)
pinot.fs.s3.region=us-west-2
```

**Maven dependency:**
```xml
<dependency>
  <groupId>org.apache.pinot</groupId>
  <artifactId>pinot-s3</artifactId>
  <version>${pinot.version}</version>
</dependency>
```

**Example URI in protocol:**
```json
{
  "version": "1.0",
  "type": "uri",
  "format": "avro",
  "uri": "s3://my-bucket/batches/batch-20250110-123456.avro"
}
```

### HDFS Configuration

**Server/Controller startup properties:**
```properties
# HDFS Filesystem
pinot.fs.hdfs.class=org.apache.pinot.plugin.filesystem.HadoopPinotFS
pinot.fs.hdfs.hadoop.conf.path=/etc/hadoop/conf
```

**Maven dependency:**
```xml
<dependency>
  <groupId>org.apache.pinot</groupId>
  <artifactId>pinot-hdfs</artifactId>
  <version>${pinot.version}</version>
</dependency>
```

**Example URI in protocol:**
```json
{
  "version": "1.0",
  "type": "uri",
  "format": "parquet",
  "uri": "hdfs://namenode:9000/user/pinot/batches/batch-001.parquet"
}
```

### Google Cloud Storage (GCS)

**Server/Controller startup properties:**
```properties
# GCS Filesystem
pinot.fs.gs.class=org.apache.pinot.plugin.filesystem.GcsPinotFS
pinot.fs.gs.projectId=my-project-id
pinot.fs.gs.gcpKey=<service-account-json>
```

**Maven dependency:**
```xml
<dependency>
  <groupId>org.apache.pinot</groupId>
  <artifactId>pinot-gcs</artifactId>
  <version>${pinot.version}</version>
</dependency>
```

**Example URI in protocol:**
```json
{
  "version": "1.0",
  "type": "uri",
  "format": "avro",
  "uri": "gs://my-gcs-bucket/batches/batch-202501.avro"
}
```

### Azure Blob Storage (ABFS)

**Server/Controller startup properties:**
```properties
# Azure Filesystem
pinot.fs.abfs.class=org.apache.pinot.plugin.filesystem.AzurePinotFS
pinot.fs.abfs.accountName=myaccount
pinot.fs.abfs.accessKey=<access-key>
```

**Maven dependency:**
```xml
<dependency>
  <groupId>org.apache.pinot</groupId>
  <artifactId>pinot-azure</artifactId>
  <version>${pinot.version}</version>
</dependency>
```

**Example URI in protocol:**
```json
{
  "version": "1.0",
  "type": "uri",
  "format": "json",
  "uri": "abfs://container@myaccount.dfs.core.windows.net/batches/batch.json"
}
```

### Local Filesystem

**Server/Controller startup properties:**
```properties
# Local Filesystem (for testing)
pinot.fs.file.class=org.apache.pinot.spi.filesystem.LocalPinotFS
```

**Example URI in protocol:**
```json
{
  "version": "1.0",
  "type": "uri",
  "format": "avro",
  "uri": "file:///data/batches/batch-test.avro"
}
```

## Supported Data Formats

### AVRO
- Format: `"avro"`
- Decoder: `org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor`
- File extension: `.avro`

### Parquet
- Format: `"parquet"`
- Decoder: `org.apache.pinot.plugin.inputformat.parquet.ParquetRecordExtractor`
- File extension: `.parquet`

### JSON
- Format: `"json"`
- Decoder: `org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor`
- File extension: `.json`

## Producer Configuration

### Java Producer Example

```java
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

// Using the MicroBatchProtocol helper
byte[] protocolMessage = MicroBatchProtocol.createUriMessage(
    "s3://my-bucket/batch-123.avro",
    MicroBatchProtocol.Format.AVRO
);

Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer");
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.ByteArraySerializer");

try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
    producer.send(new ProducerRecord<>(
        "microbatch-topic",
        "batch-123",
        protocolMessage
    ));
}
```

### Python Producer Example

```python
from kafka import KafkaProducer
import json
import base64

def create_uri_message(uri, format_type="avro"):
    return json.dumps({
        "version": "1.0",
        "type": "uri",
        "format": format_type,
        "uri": uri
    }).encode('utf-8')

def create_data_message(data_bytes, format_type="avro"):
    return json.dumps({
        "version": "1.0",
        "type": "data",
        "format": format_type,
        "data": base64.b64encode(data_bytes).decode('utf-8')
    }).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: v
)

# Send URI message
uri_message = create_uri_message("s3://bucket/batch.avro")
producer.send('microbatch-topic', key=b'batch-1', value=uri_message)

# Send inline data message
with open('batch.avro', 'rb') as f:
    data_message = create_data_message(f.read())
    producer.send('microbatch-topic', key=b'batch-2', value=data_message)

producer.flush()
```

## Best Practices

1. **File Naming**: Use timestamped or sequential batch file names for easier tracking
   - Example: `batch-{timestamp}-{sequence}.avro`

2. **File Size**: Keep batch files reasonably sized (10MB - 1GB) to balance:
   - Kafka message size limits (for inline data)
   - Download time and memory usage
   - Parallelism and throughput

3. **Cleanup**: Implement lifecycle policies on your storage:
   - S3: Use lifecycle rules to archive/delete old batches
   - HDFS: Use scheduled cleanup jobs
   - Ensure Pinot has processed batches before deletion

4. **Monitoring**: Track key metrics:
   - Kafka consumer lag
   - File download latency
   - Processing throughput
   - Failed protocol parsing attempts

5. **Error Handling**:
   - Invalid protocol messages will fail with clear error messages
   - Missing files will fail the consumer - ensure files exist before sending URIs
   - Use Kafka retries and dead letter queues for production

## Troubleshooting

### Issue: "Failed to parse microbatch protocol"
**Cause**: Kafka message doesn't follow the JSON protocol format
**Solution**: Verify message structure includes all required fields (version, type, format, uri/data)

### Issue: "Unsupported protocol version"
**Cause**: Message has version other than "1.0"
**Solution**: Ensure producer creates messages with version "1.0"

### Issue: "Failed to download file from PinotFS"
**Cause**: File doesn't exist or PinotFS not configured
**Solution**:
- Verify file exists at the URI
- Check PinotFS configuration for the URI scheme (s3://, hdfs://, etc.)
- Verify authentication/permissions

### Issue: "Invalid format"
**Cause**: Unsupported format specified
**Solution**: Use only supported formats: avro, parquet, json

## Migration from Legacy Approach

If you were previously sending raw URIs or data as Kafka messages (without JSON protocol), update your producers to use the protocol format. The microbatch module does not support legacy formats - all messages must use the JSON protocol.
