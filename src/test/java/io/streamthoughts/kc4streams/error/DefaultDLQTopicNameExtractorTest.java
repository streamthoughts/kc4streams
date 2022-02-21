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

import io.streamthoughts.kc4streams.error.internal.FailedRecordContextBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class DefaultDLQTopicNameExtractorTest {

    @Test
    public void shouldExtractTopicGivenDefaultConfig() {
        var extractor = new DefaultDLQTopicNameExtractor<String, String>();
        extractor.configure(Map.of());

        FailedRecordContext context = FailedRecordContextBuilder
                .with(new RuntimeException(), ExceptionStage.PROCESSING)
                .withTopic("input-topic")
                .withApplicationId("my-application-id")
                .build();
        String extracted = extractor.extract("key", "value", context);
        Assertions.assertEquals("input-topic.dlq.my-application-id", extracted);
    }

    @Test
    public void shouldExtractTopicGivenPerApplicationFalse() {
        var extractor = new DefaultDLQTopicNameExtractor<String, String>();
        extractor.configure(Map.of(
                DefaultDLQTopicNameExtractor.Config.DLQ_DEFAULT_TOPIC_PER_APPLICATION_ID_CONFIG, false
        ));

        FailedRecordContext context = FailedRecordContextBuilder
                .with(new RuntimeException(), ExceptionStage.PROCESSING)
                .withTopic("input-topic")
                .withApplicationId("my-application-id")
                .build();
        String extracted = extractor.extract("key", "value", context);
        Assertions.assertEquals("input-topic.dlq", extracted);
    }

    @Test
    public void shouldExtractTopicGivenCustomSuffix() {
        var extractor = new DefaultDLQTopicNameExtractor<String, String>();
        extractor.configure(Map.of(
                DefaultDLQTopicNameExtractor.Config.DLQ_DEFAULT_TOPIC_SUFFIX_CONFIG, "error"
        ));

        FailedRecordContext context = FailedRecordContextBuilder
                .with(new RuntimeException(), ExceptionStage.PROCESSING)
                .withTopic("input-topic")
                .withApplicationId("my-application-id")
                .build();
        String extracted = extractor.extract("key", "value", context);
        Assertions.assertEquals("input-topic.error.my-application-id", extracted);
    }
}