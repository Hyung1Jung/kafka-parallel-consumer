package me.kafkaparallelconsumer.listener

import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import org.springframework.kafka.core.ConsumerFactory
import java.time.Duration

class KafkaParallelStreamProcessorFactory<K, V> {

    fun createParallelStreamProcessor(
        kafkaConsumerFactory: ConsumerFactory<K, V>,
        topics: Array<String>,
        ordering: ParallelConsumerOptions.ProcessingOrder,
        concurrency: Int,
        batchSize: Int,
        groupId: String,
        clientIdPrefix: String,
        clientIdSuffix: String,
    ): ParallelStreamProcessor<K, V> {
        val options: ParallelConsumerOptions<K, V> = ParallelConsumerOptions.builder<K, V>()
            .ordering(ordering)
            .maxConcurrency(concurrency)
            .batchSize(batchSize)
            .consumer(
                kafkaConsumerFactory.createConsumer(
                    groupId.takeIf { it.isNotEmpty() },
                    clientIdPrefix,
                    clientIdSuffix,
                ),
            )
            .commitInterval(Duration.ofMillis(500))
            .build()
        val eosStreamProcessor = ParallelStreamProcessor.createEosStreamProcessor(options)
        eosStreamProcessor.subscribe(topics.toList())
        return eosStreamProcessor
    }
}
