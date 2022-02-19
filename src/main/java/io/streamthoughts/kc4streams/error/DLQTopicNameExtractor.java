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

import org.apache.kafka.common.Configurable;

import java.util.Map;

/**
 * An interface that allows to dynamically determine the name of the Kafka topic
 * to send corrupted record.
 */
public interface DLQTopicNameExtractor<K, V> extends Configurable {

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    default void configure(Map<String, ?> configs) {

    }

    /**
     * Extracts the topic name to send to. The topic will be automatically created if it does not already exist.
     *
     * @see DLQRecordCollector
     * @see DLQRecordCollectorConfig
     *
     * @param key           the record key
     * @param value         the record value
     * @param recordContext current context metadata of the record
     * @return              the topic name this record should be sent to
     */
    String extract(final K key, final V value, final FailedRecordContext recordContext);

}
