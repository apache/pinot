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
package org.apache.pinot.plugins.stream.gcppubsub;

import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;

public class PubSubMetadataProvider implements StreamMetadataProvider {
	public PubSubMetadataProvider(String clientId, StreamConfig streamConfig) {}

	@Override
	public int fetchPartitionCount(long timeoutMillis) {
		return 0;
	}

	@Override
	public long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long l) throws TimeoutException {
		throw new UnsupportedOperationException("Usage of this method is not supported with Pub/Sub");
	}

	@Override
	public StreamPartitionMsgOffset fetchStreamPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis) throws TimeoutException {
		throw new UnsupportedOperationException("Usage of this method is not supported with Pub/Sub");
	}

	@Override
	public void close() throws IOException {
		throw new UnsupportedOperationException("Usage of this method is not supported with Pub/Sub");
	}
}
