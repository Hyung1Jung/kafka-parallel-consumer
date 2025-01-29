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
        createUserMessages().forEach { userMessageProducer.sendMessage(it) }
    }

    @PostMapping("/parallel-send")
    fun parallelSend() {
        createUserMessages().forEach { userMessageProducer.sendParallelMessage(it) }
    }

    @PostMapping("/batch-parallel-send")
    fun batchParallelSend() {
        createUserMessages().forEach { userMessageProducer.sendBatchParallelMessage(it) }
    }

    private fun createUserMessages() = (1..10).map { index ->
        UserMessage(
            id = index.toLong(),
            age = index.toLong(),
            "name$index",
        )
    }
}
