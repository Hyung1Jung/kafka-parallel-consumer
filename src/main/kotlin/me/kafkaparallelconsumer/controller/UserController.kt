package me.kafkaparallelconsumer.controller

import me.kafkaparallelconsumer.model.UserMessage
import me.kafkaparallelconsumer.producer.UserMessageProducer
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class UserController(
    private val userMessageProducer: UserMessageProducer,
) {

    @PostMapping("/send")
    fun send() {
        generateUserMessages().forEach { userMessageProducer.sendMessage(it) }
    }

    @PostMapping("/parallel-send")
    fun parallelSend() {
        generateUserMessages().forEach { userMessageProducer.sendParallelMessage(it) }
    }

    @PostMapping("/batch-parallel-send")
    fun batchParallelSend() {
        generateUserMessages().forEach { userMessageProducer.sendBatchParallelMessage(it) }
    }

    private fun generateUserMessages() = (1..100).map { index ->
        UserMessage(
            id = index.toLong(),
            age = index.toLong(),
            name = "name$index",
        )
    }
}
