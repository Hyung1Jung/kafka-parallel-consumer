package me.kafkaparallelconsumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaParallelConsumerApplication

fun main(args: Array<String>) {
    runApplication<KafkaParallelConsumerApplication>(*args)
}
