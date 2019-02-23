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
package com.linkedin.pinot.opal.distributed.keyCoordinator.internal;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.opal.common.RpcQueue.KafkaQueueConsumer;
import com.linkedin.pinot.opal.common.RpcQueue.KafkaQueueProducer;
import com.linkedin.pinot.opal.common.RpcQueue.ProduceTask;
import com.linkedin.pinot.opal.common.keyValueStore.ByteArrayWrapper;
import com.linkedin.pinot.opal.common.keyValueStore.KeyValueStoreDB;
import com.linkedin.pinot.opal.common.keyValueStore.KeyValueStoreTable;
import com.linkedin.pinot.opal.common.keyValueStore.RocksDBKeyValueStoreDB;
import com.linkedin.pinot.opal.common.messages.KeyCoordinatorMessageContext;
import com.linkedin.pinot.opal.common.messages.KeyCoordinatorQueueMsg;
import com.linkedin.pinot.opal.common.messages.LogCoordinatorMessage;
import com.linkedin.pinot.opal.common.messages.LogEventType;
import com.linkedin.pinot.opal.common.updateStrategy.MessageResolveStrategy;
import com.linkedin.pinot.opal.common.utils.CommonUtils;
import com.linkedin.pinot.opal.common.utils.State;
import com.linkedin.pinot.opal.distributed.keyCoordinator.starter.KeyCoordinatorConf;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DistributedKeyCoordinatorCore {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedKeyCoordinatorCore.class);
  private static final String KAFKA_TOPIC_FORMAT = "pinot_upsert_%s";
  private static final long TERMINATION_WAIT_MS = 10000;

  private KeyCoordinatorConf _conf;
  private KafkaQueueProducer<String, LogCoordinatorMessage> _outputKafkaProducer;
  private KafkaQueueConsumer<KeyCoordinatorQueueMsg> _inputKafkaConsumer;
  private MessageResolveStrategy _messageResolveStrategy;
  private KeyValueStoreDB<byte[], byte[]> _keyValueStoreDB;
  private ExecutorService _coreThread;
  private volatile State _state = State.SHUTDOWN;

  private int fetchMsgDelayMs;
  private int fetchMsgMaxDelayMs;
  private int fetchMsgMaxCount;


  public DistributedKeyCoordinatorCore() {}

  public void init(KeyCoordinatorConf conf, KafkaQueueProducer<String, LogCoordinatorMessage> keyCoordinatorProducer,
                   KafkaQueueConsumer<KeyCoordinatorQueueMsg> keyCoordinatorConsumer,
                   MessageResolveStrategy messageResolveStrategy) {
    Preconditions.checkState(_state == State.SHUTDOWN, "can only init if it is not running yet");
    CommonUtils.printConfiguration(conf, "distributed key coordinator core");
    _conf = conf;
    _outputKafkaProducer = keyCoordinatorProducer;
    _inputKafkaConsumer = keyCoordinatorConsumer;
    _messageResolveStrategy = messageResolveStrategy;
    _keyValueStoreDB = getKeyValueStore(conf.subset(KeyCoordinatorConf.KEY_COORDINATOR_KV_STORE));
    _coreThread = Executors.newSingleThreadExecutor();

    // local local config
    fetchMsgDelayMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_DELAY_MS,
        KeyCoordinatorConf.FETCH_MSG_DELAY_MS_DEFAULT);
    fetchMsgMaxDelayMs = conf.getInt(KeyCoordinatorConf.FETCH_MSG_MAX_DELAY_MS,
        KeyCoordinatorConf.FETCH_MSG_MAX_DELAY_MS_DEFAULT);
    fetchMsgMaxCount = conf.getInt(KeyCoordinatorConf.FETCH_MSG_MAX_BATCH_SIZE,
        KeyCoordinatorConf.FETCH_MSG_MAX_BATCH_SIZE_DEFAULT);

    _state = State.INIT;
  }

  public void start() {
    Preconditions.checkState(_state == State.INIT, "key coordinate is not in correct state");
    LOGGER.info("starting key coordinator message process loop");
    _coreThread.submit(this::messageProcessLoop);
  }

  public void messageProcessLoop() {
    try {
      _state = State.RUNNING;
      LOGGER.info("starting local key coordinator");
      long flushDeadline = System.currentTimeMillis() + fetchMsgMaxDelayMs;
      List<KeyCoordinatorQueueMsg> messages = new ArrayList<>(fetchMsgMaxCount);
      while (_state == State.RUNNING) {
        // process message when we got max message count or reach max delay ms
        List<KeyCoordinatorQueueMsg> data = _inputKafkaConsumer.
            getRequests(fetchMsgMaxDelayMs, TimeUnit.MILLISECONDS);
        messages.addAll(data);

        if (messages.size() > fetchMsgMaxCount || System.currentTimeMillis() >= flushDeadline) {
          long startTime = System.currentTimeMillis();
          flushDeadline = System.currentTimeMillis() + fetchMsgMaxDelayMs;
          if (messages.size() > 0) {
            processMessages(messages);
            LOGGER.info("processed {} messages to log coordinator queue in {} ms", messages.size(),
                System.currentTimeMillis() - startTime);
            messages.clear();
          }
        }
      }
    } catch (Exception ex) {
      LOGGER.warn("key coordinator is exiting due to exception", ex);
    } finally {
      _state = State.SHUTTING_DOWN;
      LOGGER.info("exiting key coordinator loop");
    }
    LOGGER.info("existing key coordinator core /procthread");
  }

  public void stop() {
    _state = State.SHUTTING_DOWN;
    _coreThread.shutdown();
    try {
      _coreThread.awaitTermination(TERMINATION_WAIT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      LOGGER.error("failed to wait for key coordinator thread to shutdown", ex);
    }
    _coreThread.shutdownNow();
    _state = State.SHUTDOWN;
  }

  public State getState() {
    return _state;
  }

  private void processMessages(List<KeyCoordinatorQueueMsg> messages) {
    Map<String, List<KeyCoordinatorQueueMsg>> topicMsgMap = new HashMap<>();
    for (KeyCoordinatorQueueMsg msg: messages) {
      topicMsgMap.computeIfAbsent(msg.getPinotTable(), t -> new ArrayList<>()).add(msg);
    }
    for (Map.Entry<String, List<KeyCoordinatorQueueMsg>> entry: topicMsgMap.entrySet()) {
      processMessagesForTopic(entry.getKey(), entry.getValue());
    }
  }


  private void processMessagesForTopic(String tableName, List<KeyCoordinatorQueueMsg> msgList) {
    try {
      if (msgList == null || msgList.size() == 0) {
        LOGGER.warn("trying to process topic message with empty list {}", tableName);
        return;
      }
      KeyValueStoreTable<byte[], byte[]> table = _keyValueStoreDB.getTable(tableName);
      Map<ByteArrayWrapper, KeyCoordinatorMessageContext> keyValueMap = new HashMap<>(msgList.size());

      List<ProduceTask<String, LogCoordinatorMessage>> tasks = new ArrayList<>();
      for (KeyCoordinatorQueueMsg msg: msgList) {
        ByteArrayWrapper key = new ByteArrayWrapper(msg.getKey());
        if (!keyValueMap.containsKey(key) || _messageResolveStrategy.compareMessage(keyValueMap.get(key), msg.getContext())) {
          keyValueMap.put(key, msg.getContext());
        }
      }
      LOGGER.info("found total of {} duplicated keys", msgList.size() - keyValueMap.size());
      Map<byte[], byte[]> result = table.multiGet(
          keyValueMap.keySet().stream().map(ByteArrayWrapper::getData).collect(Collectors.toList()));
      for (Map.Entry<byte[], byte[]> entry: result.entrySet()) {
        emitDeleteActionForOldEvents(tableName, KeyCoordinatorMessageContext.fromBytes(entry.getValue()),
            keyValueMap.get(new ByteArrayWrapper(entry.getKey())), tasks);
      }
      for (KeyCoordinatorQueueMsg msg: msgList) {
        emitInsertActionForNewEvents(tableName, msg.getKey(), msg.getContext(), keyValueMap, tasks);
      }
//      LOGGER.info("table {}: found {} keys in kv store out of {} keys", table, foundKeyCount, msgList.size());
      LOGGER.info("sending {} tasks to log coordinator queue", tasks.size());
      long startTime = System.currentTimeMillis();
      List<ProduceTask<String, LogCoordinatorMessage>> failedTasks = sendMessagesToLogCoordinator(tasks, 10, TimeUnit.SECONDS);
      LOGGER.info("send to producer take {} ms and {} failed", System.currentTimeMillis() - startTime, failedTasks.size());

      updateKeyValueStore(table, keyValueMap);
      LOGGER.info("updated list of message to key value store");
    } catch (IOException e) {
      throw new RuntimeException("failed to interact with rocksdb", e);
    } catch (RuntimeException e) {
      throw new RuntimeException("failed to interact with key value store", e);
    }
  }

  private void emitDeleteActionForOldEvents(String tableName, Optional<KeyCoordinatorMessageContext> oldContext,
                                            KeyCoordinatorMessageContext newContext, List<ProduceTask<String, LogCoordinatorMessage>> tasks) {
    if (oldContext.isPresent() && _messageResolveStrategy.compareMessage(oldContext.get(), newContext)) {
      tasks.add(createMessageToLogCoordinator(tableName, oldContext.get().getSegmentName(), oldContext.get().getKafkaOffset(),
          newContext.getKafkaOffset(), LogEventType.DELETE));
    } else {
      tasks.add(createMessageToLogCoordinator(tableName, newContext.getSegmentName(), newContext.getKafkaOffset(),
          oldContext.get().getKafkaOffset(), LogEventType.DELETE));
    }
  }

  private void emitInsertActionForNewEvents(String table, byte[] key, KeyCoordinatorMessageContext context,
                                            Map<ByteArrayWrapper, KeyCoordinatorMessageContext> keyMap,
                                            List<ProduceTask<String, LogCoordinatorMessage>> tasks) {
    ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
    if (context != keyMap.get(keyWrapper)) {
      tasks.add(createMessageToLogCoordinator(table, context.getSegmentName(), context.getKafkaOffset(),
          keyMap.get(keyWrapper).getKafkaOffset(), LogEventType.DELETE));
    }
    tasks.add(createMessageToLogCoordinator(table, context.getSegmentName(), context.getKafkaOffset(),
        context.getKafkaOffset(), LogEventType.INSERT));
  }

  public ProduceTask<String, LogCoordinatorMessage> createMessageToLogCoordinator(String table, String segment, long oldKafkaOffset,
                                                                                  long newKafkaOffset, LogEventType eventType) {
    return new ProduceTask<>(getKafkaTopicName(table), segment,
        new LogCoordinatorMessage(table, segment, oldKafkaOffset, newKafkaOffset, eventType));
  }

  private void updateKeyValueStore(KeyValueStoreTable<byte[], byte[]> table, Map<ByteArrayWrapper, KeyCoordinatorMessageContext> keyValueMap) throws IOException {
    List<byte[]> keys = new ArrayList<>();
    List<byte[]> values = new ArrayList<>();
    for (Map.Entry<ByteArrayWrapper, KeyCoordinatorMessageContext> entry: keyValueMap.entrySet()) {
      keys.add(entry.getKey().getData());
      values.add(entry.getValue().toBytes());
    }
    table.multiPut(keys, values);
  }

  private List<ProduceTask<String, LogCoordinatorMessage>> sendMessagesToLogCoordinator(
      List<ProduceTask<String, LogCoordinatorMessage>> tasks, long timeout, TimeUnit timeUnit) {

    // send all and wait for result, batch for better perf
    CountDownLatch countDownLatch = new CountDownLatch(tasks.size());
    tasks.forEach(t -> t.setCountDownLatch(countDownLatch));
    _outputKafkaProducer.batchProduce(tasks);
    try {
      boolean allFinished = countDownLatch.await(timeout, timeUnit);
      if (allFinished) {
        return new ArrayList<>();
      } else {
        return tasks.stream().filter(t -> !t.isSucceed()).collect(Collectors.toList());
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("encountered run time exception while waiting for the loop to finish");
    }
  }

  private String getKafkaTopicName(String tableName) {
    return String.format(KAFKA_TOPIC_FORMAT, tableName);
  }

  protected KeyValueStoreDB<byte[], byte[]> getKeyValueStore(Configuration conf) {
    RocksDBKeyValueStoreDB keyValueStore = new RocksDBKeyValueStoreDB();
    keyValueStore.init(conf);
    return keyValueStore;
  }
}
