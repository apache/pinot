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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class PubSubMessageDecoder implements StreamMessageDecoder<PubsubMessage> {
	private static final String JSON_RECORD_EXTRACTOR_CLASS = "org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor";
	private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageDecoder.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private RecordExtractor<Map<String, Object>> jsonRecordExtractor;

	@Override
	public void init(Map<String, String> props, Set<String> fieldsToRead, String topicId) throws Exception {
		String recordExtractorClass = null;

		if (props != null) {
			recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
		}

		if (recordExtractorClass == null) {
			recordExtractorClass = JSON_RECORD_EXTRACTOR_CLASS;
		}

		jsonRecordExtractor = PluginManager.get().createInstance(recordExtractorClass);
		jsonRecordExtractor.init(fieldsToRead, null);
	}

	@Override
	public GenericRow decode(PubsubMessage message, GenericRow destination) {
		try {
			JsonNode jsonMessage = JsonUtils.bytesToJsonNode(message.getData().toByteArray());
			Map<String, Object> from = OBJECT_MAPPER
					.convertValue(jsonMessage, new TypeReference<Map<String, Object>>() {});
			jsonRecordExtractor.extract(from, destination);
			return destination;
		} catch (Exception exc) {
			LOGGER.error("Failed to parse bytes to JSON. Content was {}", message.getData(), exc);
			return null;
		}
	}

	@Override
	public GenericRow decode(PubsubMessage message, int offset, int length, GenericRow destination) {
		throw new UnsupportedOperationException("Pub/Sub stream plugin does not support offset decoding.");
	}
}
