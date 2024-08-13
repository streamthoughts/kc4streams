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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The DLQRecordCollector is responsible for sending records to DLQs.
 */
public class DLQRecordCollector implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DLQRecordCollector.class);

    public static final String DEFAULT_CLIENT_ID = "kafka-streams-dlq-collector-producer";

    private static volatile DLQRecordCollector INSTANCE;

    private static final AtomicBoolean CONFIGURED = new AtomicBoolean(false);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Set<String> topicCache = ConcurrentHashMap.newKeySet();

    private final DLQRecordCollectorConfig config;

    private final Producer<byte[], byte[]> producer;

    private final AdminClient adminClient;

    /**
     * Creates a new {@link DLQRecordCollector} instance.
     *
     * @param config               the {@link DLQRecordCollectorConfig}.
     * @param registerShutdownHook flag to indicate if a register shutdown hook should be registered.
     */
    private DLQRecordCollector(final DLQRecordCollectorConfig config,
                               final boolean registerShutdownHook) {
        this.config = config;
        this.producer = config.getProducer().get();
        this.adminClient = config.getAdminClient().orElse(null);
        mayRegisterShutdownHook(registerShutdownHook);
    }

    private void mayRegisterShutdownHook(final boolean registerShutdownHook) {
        if (registerShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(this::close));
            LOG.info("Registered a JVM shutdown hook for closing DLQRecordCollector.");
        }
    }

    private static KafkaProducer<byte[], byte[]> createProducer(final Map<String, Object> config) {
        return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static AdminClient createAdminClient(final Map<String, Object> config) {
        return KafkaAdminClient.create(config);
    }

    /**
     * Gets the {@link DLQRecordCollector} and create it if no one is already initialized.
     *
     * @param config the {@link DLQRecordCollectorConfig}.
     * @return the {@link DLQRecordCollector} instance.
     */
    public static synchronized DLQRecordCollector getOrCreate(final Map<String, ?> config) {
        return getOrCreate(new DLQRecordCollectorConfig(config));
    }

    /**
     * Gets the {@link DLQRecordCollector} and create it if no one is already initialized.
     *
     * @param config the {@link DLQRecordCollectorConfig}.
     * @return the {@link DLQRecordCollector} instance.
     */
    public static synchronized DLQRecordCollector getOrCreate(
            final DLQRecordCollectorConfig config
    ) {
        if (CONFIGURED.compareAndSet(false, true)) {
            if (config.getProducer().isEmpty()) {
                LOG.info("Initializing global-dead-letter-topic-producer using the supplied configuration.");
                final Map<String, Object> newProducerConfig = config.getProducerConfig();
                newProducerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
                newProducerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
                newProducerConfig.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, DEFAULT_CLIENT_ID);
                config.withProducer(createProducer(newProducerConfig));
            }

            if (config.isAutoCreateTopicEnabled() && config.getAdminClient().isEmpty()) {
                LOG.info("Initializing global-dead-letter-topic-admin-client using the supplied configuration.");
                config.withAdminClient(createAdminClient(config.getAdminClientConfig()));
            }

            INSTANCE = new DLQRecordCollector(config, true);
        }
        return INSTANCE;
    }

    /**
     * @return {@code true} if the {@link DLQRecordCollector} is created.
     */
    public static synchronized boolean isCreated() {
        return CONFIGURED.get();
    }

    /**
     * Gets the {@link DLQRecordCollector}.
     *
     * @return the {@link DLQRecordCollector}.
     * @throws IllegalStateException if no {@link DLQRecordCollector} has been created.
     */
    public static synchronized DLQRecordCollector get() {
        if (!CONFIGURED.get()) {
            throw new IllegalStateException("DLQRecordCollector is not created.");
        }
        return INSTANCE;
    }

    /**
     * Sends a {@code null} key-value record to a dead-letter topics using the given context information.
     *
     * @param topic  the name of the Dead Letter Queue.
     * @param failed the {@link Failed} record context.
     */
    public void send(final String topic,
                     final Failed failed) {
        send(topic, null, null, null, null, failed);
    }

    /**
     * Sends a {@code null} key-value record to a dead-letter topics using the given context information.
     *
     * @param topicNameExtractor the extractor to determine the name of the Kafka topic to write
     * @param failed             the {@link Failed} record context.
     */
    public <K, V> void send(final DLQTopicNameExtractor<K, V> topicNameExtractor,
                            final Failed failed) {
        var topic = topicNameExtractor.extract(null, null, failed.toFailedRecordContext());
        send(topic, null, null, null, null, failed);
    }

    /**
     * Sends a {@code null} key-value record to a dead-letter topics using the given context information.
     *
     * @param topicNameExtractor the extractor to determine the name of the Kafka topic to write
     * @param kv              the record key-value pair.
     * @param keySerializer   the {@link Serializer} for the key.
     * @param valueSerializer the {@link Serializer} for the value.
     * @param failed          the {@link Failed} record context.
     * @param <K>             the type of the key.
     * @param <V>             the type of the value.
     */
    public <K, V> void send(final DLQTopicNameExtractor<K, V> topicNameExtractor,
                            final KeyValue<K, V> kv,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final Failed failed) {
        send(topicNameExtractor, kv.key, kv.value, keySerializer, valueSerializer, failed);
    }

    /**
     * Sends a key-value record to a DLQ using the given context information.
     *
     * @param topic           the name of the Dead Letter Topic.
     * @param kv              the record key-value pair.
     * @param keySerializer   the {@link Serializer} for the key.
     * @param valueSerializer the {@link Serializer} for the value.
     * @param failed          the {@link Failed} record context.
     * @param <K>             the type of the key.
     * @param <V>             the type of the value.
     */
    public <K, V> void send(final String topic,
                            final KeyValue<K, V> kv,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final Failed failed) {
        send(topic, kv.key, kv.value, keySerializer, valueSerializer, failed);
    }

    /**
     * Sends a {@code null} key-value record to a dead-letter topics using the given context information.
     *
     * @param topicNameExtractor the extractor to determine the name of the Kafka topic to write
     * @param key             the record-key.
     * @param value           the record-value.
     * @param keySerializer   the {@link Serializer} for the key.
     * @param valueSerializer the {@link Serializer} for the value.
     * @param failed          the {@link Failed} record context.
     * @param <K>             the type of the key.
     * @param <V>             the type of the value.
     */
    public <K, V> void send(final DLQTopicNameExtractor<K, V> topicNameExtractor,
                            final K key,
                            final V value,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final Failed failed) {
        FailedRecordContext context = failed.toFailedRecordContext();
        var topic = topicNameExtractor.extract(key, value, context);
        send(topic, key, value, keySerializer, valueSerializer, failed);
    }

    /**
     * Sends a key-value record to a DLQ using the given context information.
     *
     * @param topic           the name of the Dead Letter Topic.
     * @param key             the record-key.
     * @param value           the record-value.
     * @param keySerializer   the {@link Serializer} for the key.
     * @param valueSerializer the {@link Serializer} for the value.
     * @param failed          the {@link Failed} record context.
     * @param <K>             the type of the key.
     * @param <V>             the type of the value.
     */
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final Failed failed) {

        final byte[] keyBytes = Optional.ofNullable(key)
                .map(o -> keySerializer.serialize(topic, key))
                .orElse(null);

        final byte[] valueBytes = Optional.ofNullable(value)
                .map(o -> valueSerializer.serialize(topic, value))
                .orElse(null);

        final ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(
                        topic,
                        null,
                        failed.recordTimestamp().orElse(null),
                        keyBytes,
                        valueBytes,
                        ExceptionHeaders.addExceptionHeaders(failed.headers(), failed)
                );
        send(record);
    }

    public void send(final ProducerRecord<byte[], byte[]> record) {
        Objects.requireNonNull(record, "'record' should be null");
        send(
                record,
                (metadata, exception) -> {
                    if (exception != null) {
                        LOG.error(
                                "Failed to send record into DLQ: topic={}. Ignored record.",
                                record.topic(),
                                exception);
                    } else {
                        LOG.debug(
                                "Sent record successfully into DLQ: topic={}, partition={}, offset={}",
                                metadata.topic(),
                                metadata.partition(),
                                metadata.hasOffset() ? metadata.offset() : -1);
                    }
                });
    }

    public void send(final ProducerRecord<byte[], byte[]> record, final Callback callback) {
        doSend(record, callback);
    }

    private void doSend(final ProducerRecord<byte[], byte[]> record, final Callback callback) {
        final String topic = record.topic();
        try {
            if (config.isAutoCreateTopicEnabled() && !topicCache.contains(topic)) {
                final NewTopic newTopic = new NewTopic(
                        topic,
                        config.getTopicPartitions(),
                        config.getReplicationFactor()
                );
                if (createTopic(adminClient, newTopic)) {
                    LOG.info("DLQ Topic '{}' created successfully", newTopic);
                    topicCache.add(topic);
                }
            }
            producer.send(record, callback);
        } catch (AuthenticationException | AuthorizationException e) {
            // Can't recover from these exceptions,
            // so our only option is to close the producer and exit.
            producer.close();
            throw e; // This is fatal errors, just re-throw it.

            // TimeoutException or any error that does not belong to the public Kafka API exceptions
        } catch (KafkaException e) {
            LOG.error(
                    "Failed to send corrupted record into topic {}. Ignored record: {}",
                    topic,
                    e.getMessage()
            );
        }
    }

    public static void stop() {
        Optional.ofNullable(INSTANCE).ifPresent(DLQRecordCollector::close);
    }

    /* For Testing Purpose */
    static synchronized void clear() {
        if (CONFIGURED.compareAndSet(true, false)) {
            stop();
            INSTANCE = null;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            LOG.info("Closing {}.", getClass().getName());
            try {
                if (producer != null) producer.close(Duration.ofSeconds(5));
                if (adminClient != null) adminClient.close(Duration.ofSeconds(5));
                topicCache.clear();
                LOG.info("{} closed.", getClass().getName());
            } catch (InterruptException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static boolean createTopic(final AdminClient adminClient, final NewTopic topic) {
        try {
            CreateTopicsResult result = adminClient.createTopics(List.of(topic));
            KafkaFuture<Void> future = result.all();
            future.get();
            return true;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TopicExistsException) {
                LOG.debug("Failed to created topic '{}'. Topic already exists.", topic);
                return true;
            } else {
                LOG.info("Failed to create topic '{}'", topic, e);
                return false;
            }
        } catch (InterruptedException e) {
            LOG.info("Failed to create topic '{}'", topic, e);
            return false;
        }
    }
}
