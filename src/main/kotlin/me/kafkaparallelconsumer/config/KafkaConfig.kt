package me.kafkaparallelconsumer.config

import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.ExponentialBackOff

@Configuration
@EnableKafka
class KafkaConfig(
    private val kafkaProperties: KafkaProperties,
) {
    @Bean
    @Primary
    fun consumerFactory(): ConsumerFactory<String, Any> {
        return DefaultKafkaConsumerFactory(kafkaProperties.buildConsumerProperties())
    }

    @Bean
    @Primary
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, Any>
    ): ConcurrentKafkaListenerContainerFactory<String, Any> {
        return ConcurrentKafkaListenerContainerFactory<String, Any>().apply {
            this.consumerFactory = consumerFactory
            this.setCommonErrorHandler(DefaultErrorHandler(generateBackOff()))
            this.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        }
    }

    @Bean
    @Qualifier("batchConsumerFactory")
    fun batchConsumerFactory(): ConsumerFactory<String, Any> {
        val batchProps = kafkaProperties.buildConsumerProperties().toMutableMap()
        batchProps[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "100"
        return DefaultKafkaConsumerFactory(batchProps)
    }

    @Bean
    @Qualifier("batchKafkaListenerContainerFactory")
    fun batchKafkaListenerContainerFactory(
        @Qualifier("batchConsumerFactory") batchConsumerFactory: ConsumerFactory<String, Any>,
    ): ConcurrentKafkaListenerContainerFactory<String, Any> {
        return ConcurrentKafkaListenerContainerFactory<String, Any>().apply {
            this.consumerFactory = batchConsumerFactory
            this.isBatchListener = true
            this.containerProperties.ackMode = ContainerProperties.AckMode.BATCH
        }
    }

    @Bean
    @Primary
    fun producerFactory(): ProducerFactory<String, Any> {
        return DefaultKafkaProducerFactory(kafkaProperties.buildProducerProperties())
    }

    @Bean
    @Primary
    fun kafkaTemplate(): KafkaTemplate<String, Any> {
        return KafkaTemplate(producerFactory())
    }

    @Bean
    fun parallelKafkaConsumer(
        @Value("\${spring.kafka.parallel-consumer.group-id}") groupId: String,
        @Value("\${spring.kafka.parallel-consumer.max-poll-records}") maxPollRecords: String,
        @Value("\${spring.kafka.parallel-consumer.auto-offset-reset}") autoOffsetReset: String,
        @Value("\${spring.kafka.parallel-consumer.enable-auto-commit}") enableAutoCommit: Boolean,
    ): KafkaConsumer<String, Any> {
        val parallelConsumerProps = kafkaProperties.buildConsumerProperties().toMutableMap()
        parallelConsumerProps[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        parallelConsumerProps[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = maxPollRecords
        parallelConsumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = autoOffsetReset
        parallelConsumerProps[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = enableAutoCommit.toString()
        return KafkaConsumer(parallelConsumerProps)
    }

    @Bean
    fun parallelConsumer(parallelKafkaConsumer: KafkaConsumer<String, Any>): ParallelStreamProcessor<String, Any> {
        val options = ParallelConsumerOptions.builder<String, Any>()
            .consumer(parallelKafkaConsumer)
            .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
            .maxConcurrency(3)
            .build()
        return ParallelStreamProcessor.createEosStreamProcessor(options)
    }

    private fun generateBackOff(): BackOff {
        return ExponentialBackOff(1000, 2.0).apply {
            maxElapsedTime = 10000
        }
    }
}
