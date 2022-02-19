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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DLQExceptionHandlerConfigTest {

  @Test
  public void should_return_defaults_given_empty_configs() {
    var config =
        new DLQExceptionHandlerConfig(Map.of(), ExceptionType.DESERIALIZATION);
    assertTrue(config.customHeaders().isEmpty());
    assertTrue(config.getIgnoredExceptions().isEmpty());
    assertTrue(config.getFatalExceptions().isEmpty());
    assertNull(config.defaultHandlerResponseOrElse(null));
    assertNotNull(config.topicNameExtractor());
    assertTrue(config.topicNameExtractor().getClass().isAssignableFrom(DefaultDLQTopicNameExtractor.class));
  }

  @Test
  public void should_returns_configured_values_given_non_empty_configs() {
    var config =
        new DLQExceptionHandlerConfig(
            Map.of(
                DLQExceptionHandlerConfig.DLQ_DEFAULT_CONTINUE_ERRORS_CONFIG,
                RecordTooLargeException.class.getName(),
                DLQExceptionHandlerConfig.DLQ_DEFAULT_FAIL_ERRORS_CONFIG,
                AuthorizationException.class.getName(),
                DLQExceptionHandlerConfig.DLQ_DEFAULT_RESPONSE_CONFIG,
                ExceptionHandlerResponse.CONTINUE.name()),
            ExceptionType.DESERIALIZATION);

    assertEquals(ExceptionHandlerResponse.CONTINUE, config.defaultHandlerResponseOrElse(null));

    final Set<Class<?>> fatalExceptions = config.getFatalExceptions();
    assertEquals(1, fatalExceptions.size());
    assertEquals(AuthorizationException.class, fatalExceptions.iterator().next());

    final Set<Class<?>> ignoredExceptions = config.getIgnoredExceptions();
    assertEquals(1, ignoredExceptions.size());
    assertEquals(RecordTooLargeException.class, ignoredExceptions.iterator().next());
  }

  @Test
  public void should_returns_overridden_topic_given_non_empty_configs_for_deserialization() {
    var config =
        new DLQExceptionHandlerConfig(
            Map.of(
                DLQExceptionHandlerConfig.DLQ_DEFAULT_RESPONSE_CONFIG,
                ExceptionHandlerResponse.CONTINUE.name(),
                DLQExceptionHandlerConfig.prefixForDeserializationHandler(DLQExceptionHandlerConfig.DLQ_RESPONSE_CONFIG),
                ExceptionHandlerResponse.FAIL.name()
            ),
            ExceptionType.DESERIALIZATION
        );
    assertEquals(ExceptionHandlerResponse.FAIL, config.defaultHandlerResponseOrElse(null));
  }

  @Test
  public void should_returns_overridden_topic_given_non_empty_configs_for_production() {
    var config =
            new DLQExceptionHandlerConfig(
                    Map.of(
                            DLQExceptionHandlerConfig.DLQ_DEFAULT_RESPONSE_CONFIG,
                            ExceptionHandlerResponse.CONTINUE.name(),
                            DLQExceptionHandlerConfig.prefixForProductionHandler(DLQExceptionHandlerConfig.DLQ_RESPONSE_CONFIG),
                            ExceptionHandlerResponse.FAIL.name()
                    ),
                    ExceptionType.PRODUCTION
            );
    assertEquals(ExceptionHandlerResponse.FAIL, config.defaultHandlerResponseOrElse(null));
  }

  @Test
  public void should_throw_given_invalid_configs() {

    var exception =
        assertThrows(
            ConfigException.class,
            () -> new DLQExceptionHandlerConfig(
                Map.of(
                    DLQExceptionHandlerConfig.DLQ_DEFAULT_CONTINUE_ERRORS_CONFIG,
                    RecordTooLargeException.class.getName(),
                    DLQExceptionHandlerConfig.DLQ_DEFAULT_FAIL_ERRORS_CONFIG,
                    RecordTooLargeException.class.getName()),
                    ExceptionType.DESERIALIZATION
            ));

    Assertions.assertTrue(exception.getMessage().contains(RecordTooLargeException.class.getName()));
  }
}
