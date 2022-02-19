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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This {@link ProductionExceptionHandler} can be used to send a result record to a dedicated Dead Letter Queue (DLQ)
 * when an exception is thrown while attempting to produce to a Kafka topic.
 */
public class DLQProductionExceptionHandler
        extends AbstractDLQExceptionHandler implements ProductionExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DLQProductionExceptionHandler.class);

    /**
     * Creates a new {@link DLQProductionExceptionHandler} instance.
     */
    public DLQProductionExceptionHandler() {
        super(ExceptionType.PRODUCTION);
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
    public ProductionExceptionHandlerResponse handle(
            final ProducerRecord<byte[], byte[]> record, final Exception exception) {

        final ExceptionHandlerResponse response =
                getHandlerResponseForExceptionOrElse(exception, ExceptionHandlerResponse.FAIL);

        LOG.error(
                "Failed to produce output record to '{}-{}': {}.",
                record.topic(),
                record.partition(),
                response,
                exception
        );

        if (DLQRecordCollector.isCreated()) {
            final Failed failed = Failed
                    .withProductionError(applicationId(), exception)
                    .withRecordTopic(record.topic())
                    .withRecordPartition(record.partition())
                    .withRecordTimestamp(record.timestamp())
                    .withRecordType(Failed.RecordType.SINK)
                    .withRecordHeaders(record.headers());

            ByteArraySerde serde = new ByteArraySerde();
            DLQRecordCollector.get().send(
                    config().topicNameExtractor(),
                    record.key(),
                    record.value(),
                    serde.serializer(),
                    serde.serializer(),
                    failed

            );
        } else {
            LOG.warn("Failed to send corrupted record to Dead Letter Topic. "
                    + "DLQRecordCollector is not initialized.");
        }

        switch (response) {
            case CONTINUE:
                return ProductionExceptionHandlerResponse.CONTINUE;
            case FAIL:
                return ProductionExceptionHandlerResponse.FAIL;
            default:
                throw new IllegalArgumentException("Unsupported ProductionExceptionHandlerResponse: " + response);
        }
    }

}
