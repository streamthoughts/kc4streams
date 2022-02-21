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
package io.streamthoughts.kc4streams.error.internal;

import io.streamthoughts.kc4streams.error.ExceptionStage;
import io.streamthoughts.kc4streams.error.FailedRecordContext;
import org.apache.kafka.common.header.Headers;

import java.util.Optional;

final class InternalFailedRecordContext implements FailedRecordContext {

    private final Throwable exception;
    private final ExceptionStage exceptionType;
    private final Long offset;
    private final Long timestamp;
    private final Integer partition;
    private final String topic;
    private final Headers headers;
    private final String applicationId;

    /**
     * Creates a new {@link InternalFailedRecordContext} instance.
     */
    InternalFailedRecordContext(final Throwable exception,
                                final ExceptionStage exceptionType,
                                final Long offset,
                                final Long timestamp,
                                final Integer partition,
                                final String topic,
                                final Headers headers,
                                final String applicationId) {
        this.exception = exception;
        this.exceptionType = exceptionType;
        this.offset = offset;
        this.timestamp = timestamp;
        this.partition = partition;
        this.topic = topic;
        this.headers = headers;
        this.applicationId = applicationId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Throwable exception() {
        return exception;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExceptionStage exceptionType() {
        return exceptionType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String applicationId() {
        return applicationId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long offset() {
        return Optional.ofNullable(offset).orElse(-1L);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long timestamp() {
        return Optional.ofNullable(timestamp).orElse(-1L);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String topic() {
        return topic;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int partition() {
        return Optional.ofNullable(partition).orElse(-1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Headers headers() {
        return headers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "exception=" + exception +
                ", exceptionType=" + exceptionType +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                ", partition=" + partition +
                ", topic=" + topic +
                ", headers=" + headers +
                ", applicationId=" + applicationId +
                ']';
    }
}
