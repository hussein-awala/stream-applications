package com.stream.apps.lib.utils.fields;

import java.lang.annotation.*;

@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaAvroProducerField {}
