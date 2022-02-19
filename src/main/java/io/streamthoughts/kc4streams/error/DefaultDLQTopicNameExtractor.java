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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Optional;

public class DefaultDLQTopicNameExtractor<K, V> implements DLQTopicNameExtractor<K, V> {

    public static final String DEFAULT_SUFFIX = "dlq";

    private Config config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        config = new Config(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String extract(final K key,
                          final V value,
                          final FailedRecordContext recordContext) {
        if (config.getTopic().isPresent())
            return config.getTopic().get();

        return Optional
                .ofNullable(recordContext.topic())
                .map(topic -> topic + "." + config.getSuffix())
                .map(topic -> config.isTopicPerApplicationId() ? topic + "." + recordContext.applicationId() : topic)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Failed to extract DLQ name using "
                                + " context=" + recordContext
                                + ", configured topic='" + config.getTopic() + "'"
                                + ", configured topi-per-application-id='" + config.isTopicPerApplicationId() + "'"
                                + ", configured topic-suffix='" + config.getSuffix() + "'"
                ));
    }

    public static class Config extends AbstractConfig {

        private static final String GROUP = "Default Dead Letter Topic Name";

        public static final String DLQ_DEFAULT_TOPIC_SUFFIX_CONFIG = DLQExceptionHandlerConfig.DLQ_DEFAULT_PREFIX_CONFIG + "topic-suffix";
        private static final String DLQ_DEFAULT_TOPIC_SUFFIX_DOC = "Specifies the suffix to be used for naming the DLQ (default: 'error').";

        public static final String DLQ_DEFAULT_TOPIC_NAME_CONFIG = DLQExceptionHandlerConfig.DLQ_DEFAULT_PREFIX_CONFIG + "topic-name";
        private static final String DLQ_DEFAULT_TOPIC_NAME_DOC = "Specifies the name of the DLQ.";

        public static final String DLQ_DEFAULT_TOPIC_PER_APPLICATION_ID_CONFIG = DLQExceptionHandlerConfig.DLQ_DEFAULT_PREFIX_CONFIG + "topic-per-application-id";
        private static final String DLQ_DEFAULT_TOPIC_PER_APPLICATION_ID_DOC = "Specifies whether the application-id for Kafka Streams should be used for naming the DLQ.";

        /**
         * Creates a new {@link Config} instance.
         *
         * @param originals the originals config.
         */
        public Config(final Map<?, ?> originals) {
            super(configDef(), originals, false);
        }

        public String getSuffix() {
            return getString(DLQ_DEFAULT_TOPIC_SUFFIX_CONFIG);
        }

        public Optional<String> getTopic() {
            return Optional.ofNullable(getString(DLQ_DEFAULT_TOPIC_NAME_CONFIG));
        }

        public boolean isTopicPerApplicationId() {
            return getBoolean(DLQ_DEFAULT_TOPIC_PER_APPLICATION_ID_CONFIG);
        }

        public static ConfigDef configDef() {
            int orderInGroup = 0;
            return new ConfigDef()
                    .define(
                            DLQ_DEFAULT_TOPIC_SUFFIX_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_SUFFIX,
                            ConfigDef.Importance.HIGH,
                            DLQ_DEFAULT_TOPIC_SUFFIX_DOC,
                            GROUP,
                            orderInGroup++,
                            ConfigDef.Width.NONE,
                            DLQ_DEFAULT_TOPIC_SUFFIX_CONFIG
                    )
                    .define(
                            DLQ_DEFAULT_TOPIC_PER_APPLICATION_ID_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            true,
                            ConfigDef.Importance.HIGH,
                            DLQ_DEFAULT_TOPIC_PER_APPLICATION_ID_DOC,
                            GROUP,
                            orderInGroup++,
                            ConfigDef.Width.NONE,
                            DLQ_DEFAULT_TOPIC_PER_APPLICATION_ID_DOC
                    )
                    .define(
                            DLQ_DEFAULT_TOPIC_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            DLQ_DEFAULT_TOPIC_NAME_DOC,
                            GROUP,
                            orderInGroup++,
                            ConfigDef.Width.NONE,
                            DLQ_DEFAULT_TOPIC_NAME_DOC
                    );
        }
    }
}
