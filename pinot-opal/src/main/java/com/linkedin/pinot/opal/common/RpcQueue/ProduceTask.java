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
package com.linkedin.pinot.opal.common.RpcQueue;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.CountDownLatch;

public class ProduceTask<K, V> {
  private boolean _completed = false;
  private Exception _exception = null;
  private CountDownLatch _countDownLatch = null;
  private String _topic;
  private K _key;
  private V _value;

  public ProduceTask(K key, V value) {
    this._key = key;
    this._value = value;
  }

  public ProduceTask(String topic, K key, V value) {
    this._topic = topic;
    this._key = key;
    this._value = value;
  }

  public String getTopic() {
    return _topic;
  }

  public K getKey() {
    return _key;
  }

  public V getValue() {
    return _value;
  }

  public void setCountDownLatch(CountDownLatch countDownLatch) {
    this._countDownLatch = countDownLatch;
  }

  public boolean isSucceed() {
    return this._completed && this._exception == null;
  }

  public Exception getException() {
    return this._exception;
  }

  public synchronized void markComplete(RecordMetadata metadata, Exception exception) {
    if (exception == null) {
      markComplete();
    } else {
      markException(exception);
    }
  }

  public synchronized void markComplete() {
    if (!this._completed) {
      this._completed = true;
      if (_countDownLatch != null) {
        this._countDownLatch.countDown();
      }
    }
  }

  public synchronized void markException(Exception exception) {
    if (!this._completed) {
      this._completed = true;
      this._exception = exception;
      if (_countDownLatch != null) {
        this._countDownLatch.countDown();
      }
    }
  }
}
