package me.kafkaparallelconsumer.listener

import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import org.springframework.kafka.core.ConsumerFactory

class KafkaParallelStreamProcessorFactory<K, V> {

    fun createParallelStreamProcessor(
        kafkaConsumerFactory: ConsumerFactory<K, V>,
        topics: Array<String>,
        ordering: ParallelConsumerOptions.ProcessingOrder,
        maxConcurrency: Int,
        batchSize: Int,
        groupId: String,
        clientIdPrefix: String,
        clientIdSuffix: String,
    ): ParallelStreamProcessor<K, V> {
        val options: ParallelConsumerOptions<K, V> = ParallelConsumerOptions.builder<K, V>()
            .ordering(ordering)
            .maxConcurrency(maxConcurrency)
            .batchSize(batchSize)
            .consumer(
                kafkaConsumerFactory.createConsumer(
                    groupId.takeIf { it.isNotEmpty() },
                    clientIdPrefix,
                    clientIdSuffix,
                ),
            )
            .build()
        val eosStreamProcessor = ParallelStreamProcessor.createEosStreamProcessor(options)
        eosStreamProcessor.subscribe(topics.toList())
        return eosStreamProcessor
    }
}
