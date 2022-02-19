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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This {@link StreamsUncaughtExceptionHandler} can be used to send record to a dedicated Dead Letter Queue (DLQ).
 */
public class DLQStreamUncaughtExceptionHandler
        extends AbstractDLQExceptionHandler implements StreamsUncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DLQStreamUncaughtExceptionHandler.class);

    /**
     * Creates a new {@link DLQStreamUncaughtExceptionHandler} instance.
     */
    public DLQStreamUncaughtExceptionHandler() {
        super(ExceptionType.STREAM);
    }

    /**
     * Creates a new {@link DLQStreamUncaughtExceptionHandler} instance.
     */
    public DLQStreamUncaughtExceptionHandler(final Map<String, ?> configProps) {
        super(ExceptionType.STREAM);
        configure(configProps);
    }

    /**
     * Creates a new {@link DLQStreamUncaughtExceptionHandler} instance.
     */
    public DLQStreamUncaughtExceptionHandler(final StreamsConfig streamsConfig) {
        super(ExceptionType.STREAM);
        configure(streamsConfig.originals());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamThreadExceptionResponse handle(final Throwable exception) {

        if (DLQRecordCollector.isCreated()) {
            final Failed failed = Failed.withStreamError(applicationId(), exception);
            DLQTopicNameExtractor<?, ?> topicNameExtractor = config().topicNameExtractor();
            DLQRecordCollector.get().send(topicNameExtractor, failed);
        } else {
            LOG.warn("Failed to send corrupted record to Dead Letter Topic. "
                    + "DLQRecordCollector is not initialized.");
        }

        final ExceptionHandlerResponse response = getHandlerResponseForExceptionOrElse(
                exception,
                ExceptionHandlerResponse.CONTINUE
        );

        return switch (response) {
            case REPLACE_THREAD -> StreamThreadExceptionResponse.REPLACE_THREAD;
            case SHUTDOWN_CLIENT -> StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            case SHUTDOWN_APPLICATION -> StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            default -> throw new IllegalArgumentException("Unsupported StreamThreadExceptionResponse: " + response);
        };
    }
}
