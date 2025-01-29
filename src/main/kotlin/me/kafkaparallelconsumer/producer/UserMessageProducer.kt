package me.kafkaparallelconsumer.producer

import com.fasterxml.jackson.databind.ObjectMapper
import me.kafkaparallelconsumer.model.Topic
import me.kafkaparallelconsumer.model.UserMessage
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class UserMessageProducer(
    private val kafkaTemplate: KafkaTemplate<String, Any>,
    private val objectMapper: ObjectMapper,
) {
    fun sendMessage(message: UserMessage) {
        val key = message.id.toString()
        val value = objectMapper.writeValueAsString(message)
        kafkaTemplate.send(Topic.USER_TOPIC, key, value)
    }

    fun sendParallelMessage(message: UserMessage) {
        val key = message.id.toString()
        val value = objectMapper.writeValueAsString(message)
        kafkaTemplate.send(Topic.PARALLEL_USER_TOPIC, key, value)
    }

    fun sendBatchParallelMessage(message: UserMessage) {
        val key = message.id.toString()
        val value = objectMapper.writeValueAsString(message)
        kafkaTemplate.send(Topic.BATCH_PARALLEL_USER_TOPIC, key, value)
    }
}
