package me.kafkaparallelconsumer.listener

import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import org.springframework.kafka.core.ConsumerFactory

class KafkaParallelConsumerFactory<K, V> {

    fun createConsumerProcessor(
        kafkaConsumerFactory: ConsumerFactory<K, V>,
        topics: Array<String>,
        ordering: ParallelConsumerOptions.ProcessingOrder = ParallelConsumerOptions.ProcessingOrder.KEY,
        maxConcurrency: Int = 3,
        groupId: String,
        clientIdPrefix: String,
        clientIdSuffix: String,
    ): ParallelStreamProcessor<K, V> {
        val options: ParallelConsumerOptions<K, V> = ParallelConsumerOptions.builder<K, V>()
            .ordering(ordering)
            .maxConcurrency(maxConcurrency)
            .consumer(
                kafkaConsumerFactory.createConsumer(
                    groupId.takeIf { it.isNotEmpty() },
                    clientIdPrefix,
                    clientIdSuffix,
                ),
            )
            .build()

        val processor = ParallelStreamProcessor.createEosStreamProcessor(options)
        processor.subscribe(topics.toList())
        return processor
    }
}
