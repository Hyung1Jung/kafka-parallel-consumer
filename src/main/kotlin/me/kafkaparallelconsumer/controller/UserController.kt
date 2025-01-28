package me.kafkaparallelconsumer.controller

import me.kafkaparallelconsumer.model.UserMessage
import me.kafkaparallelconsumer.producer.UserMessageProducer
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class UserController(
    private val userMessageProducer: UserMessageProducer,
) {

    @PostMapping("/test")
    fun test() {
        createUserMessages().forEach { userMessageProducer.sendMessage(it) }
    }

    @PostMapping("/parallel-test")
    fun parallelTest() {
        createUserMessages().forEach { userMessageProducer.sendParallelMessage(it) }
    }

    private fun createUserMessages() = (1..10).map { index ->
        UserMessage(
            id = index.toLong() % 2,
            age = index.toLong(),
            "name$index",
        )
    }
}
