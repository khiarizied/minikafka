package com.example;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for marking methods as Kafka message listeners.
 * Methods annotated with this annotation will be automatically registered
 * to consume messages from the specified topic.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KafkaListener {
    /**
     * The topic to listen to
     */
    String topic();

    /**
     * The consumer group ID
     */
    String groupId() default "default-group";

    /**
     * The partition to listen to (optional)
     * If not specified, will listen to all partitions
     */
    int partition() default -1;
}
