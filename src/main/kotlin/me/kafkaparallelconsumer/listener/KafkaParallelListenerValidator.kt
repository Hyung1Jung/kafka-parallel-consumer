package me.kafkaparallelconsumer.listener

import org.springframework.stereotype.Component

@Component
class KafkaParallelListenerValidator {

    fun validate(kafkaParallelListener: KafkaParallelListener) {
        validateMaxConcurrency(kafkaParallelListener.maxConcurrency)
        validateBatchSize(kafkaParallelListener.batchSize)
    }

    private fun validateMaxConcurrency(maxConcurrency: Int) {
        require(maxConcurrency > 0) { "maxConcurrency must be greater than 0" }
    }

    private fun validateBatchSize(batchSize: Int) {
        require(batchSize > 0) { "batchSize must be greater than 0" }
    }
}
