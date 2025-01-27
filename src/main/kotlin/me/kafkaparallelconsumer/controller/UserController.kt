package me.kafkaparallelconsumer.controller

import me.kafkaparallelconsumer.model.UserMessage
import me.kafkaparallelconsumer.producer.UserProducer
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class UserController(
    private val userProducer: UserProducer,
) {

    @PostMapping("/test")
    fun test() {
        createUserMessages().forEach { userProducer.sendMessage(it) }
    }

    @PostMapping("/parallel-test")
    fun parallelTest() {
        createUserMessages().forEach { userProducer.sendParallelMessage(it) }
    }

    private fun createUserMessages() = (1..10).map { index ->
        UserMessage(
            id = 1L,
            age = index.toLong(),
            "name$index",
        )
    }
}
