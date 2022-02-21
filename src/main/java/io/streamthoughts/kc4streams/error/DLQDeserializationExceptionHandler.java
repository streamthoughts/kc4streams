/*
 * Copyright 2022 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kc4streams.error;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.streamthoughts.kc4streams.error.ExceptionStage.DESERIALIZATION;

/**
 * This {@link DeserializationExceptionHandler} can be used to send a corrupted record to a dedicated Dead Letter Queue (DLQ)
 * when an exception thrown while attempting to deserialize it.
 */
public class DLQDeserializationExceptionHandler
        extends AbstractDLQExceptionHandler implements DeserializationExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DLQDeserializationExceptionHandler.class);

    /**
     * Creates a new {@link DLQDeserializationExceptionHandler} instance.
     */
    public DLQDeserializationExceptionHandler() {
        super(DESERIALIZATION);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DeserializationHandlerResponse handle(final ProcessorContext context,
                                                 final ConsumerRecord<byte[], byte[]> record,
                                                 final Exception exception) {

        if (DLQRecordCollector.isCreated()) {
            final Failed failed = Failed.withDeserializationError(applicationId(), exception)
                    .withRecordType(Failed.RecordType.SOURCE)
                    .withRecordTopic(record.topic())
                    .withRecordPartition(record.partition())
                    .withRecordOffset(record.offset())
                    .withRecordTimestamp(record.timestamp())
                    .withRecordHeaders(record.headers());

            Serdes.ByteArraySerde serde = new Serdes.ByteArraySerde();
            DLQRecordCollector.get().send(
                    config().topicNameExtractor(),
                    record.key(),
                    record.value(),
                    serde.serializer(),
                    serde.serializer(),
                    failed
            );
        } else {
            LOG.warn("Failed to send corrupted record to Dead Letter Queue. "
                    + "DLQRecordCollector is not initialized.");
        }

        final ExceptionHandlerResponse response = getHandlerResponseForExceptionOrElse(
                exception,
                ExceptionHandlerResponse.CONTINUE
        );

        return switch (response) {
            case CONTINUE -> DeserializationHandlerResponse.CONTINUE;
            case FAIL -> DeserializationHandlerResponse.FAIL;
            default -> throw new IllegalArgumentException("Unsupported DeserializationHandlerResponse: " + response);
        };
    }
}
