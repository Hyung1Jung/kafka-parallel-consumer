package me.kafkaparallelconsumer.listener

import org.springframework.stereotype.Component

@Component
class KafkaParallelListenerValidator {

    fun validate(kafkaParallelListener: KafkaParallelListener) {
        validateConcurrency(kafkaParallelListener.concurrency)
        validateBatchSize(kafkaParallelListener.batchSize)
    }

    private fun validateConcurrency(concurrency: Int) {
        require(concurrency > 0) { "concurrency must be greater than 0" }
    }

    private fun validateBatchSize(batchSize: Int) {
        require(batchSize > 0) { "batchSize must be greater than 0" }
    }
}
