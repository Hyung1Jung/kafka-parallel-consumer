package me.kafkaparallelconsumer.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.parallelconsumer.ParallelConsumerOptions
import me.kafkaparallelconsumer.listener.KafkaParallelListener
import me.kafkaparallelconsumer.model.Topic.BATCH_PARALLEL_USER_TOPIC
import me.kafkaparallelconsumer.model.Topic.PARALLEL_USER_TOPIC
import me.kafkaparallelconsumer.model.Topic.USER_TOPIC
import me.kafkaparallelconsumer.model.UserMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class UserMessageConsumer(
    private val objectMapper: ObjectMapper,
) {

    @KafkaListener(
        topics = [USER_TOPIC],
        groupId = "user-consumer-group",
        concurrency = "2"
    )
    fun listen(
        record: ConsumerRecord<String, String>,
        acknowledgment: Acknowledgment,
    ) {
        try {
            val message = objectMapper.readValue(record.value(), UserMessage::class.java)
            log.info("[Thread ${Thread.currentThread().id}] Partition ${record.partition()} - Message - $message")
            Thread.sleep(1000)
            acknowledgment.acknowledge()
        } catch (e: InterruptedException) {
            e.printStackTrace()
            log.info(e.message)
        }
    }

    @KafkaParallelListener(
        topics = [PARALLEL_USER_TOPIC],
        groupId = "parallel-user-consumer-group",
        concurrency = 2,
        ordering = ParallelConsumerOptions.ProcessingOrder.UNORDERED
    )
    fun listen(
        record: ConsumerRecord<String, String>,
    ) {
        try {
            val message = objectMapper.readValue(record.value(), UserMessage::class.java)
            log.info("[Thread ${Thread.currentThread().id}] Partition ${record.partition()} - Message - $message")
            Thread.sleep(1000)
        } catch (e: InterruptedException) {
            e.printStackTrace()
            log.info(e.message)
        }
    }

    @KafkaParallelListener(
        topics = [BATCH_PARALLEL_USER_TOPIC],
        groupId = "batch-parallel-user-consumer-group",
        concurrency = 1,
        batchSize = 10,
        ordering = ParallelConsumerOptions.ProcessingOrder.UNORDERED
    )
    fun batchListen(
        records: List<ConsumerRecord<String, String>>,
    ) {
        try {
            val messages = records.map { it.value() }
            val partitions = records.map { it.partition() }
            log.info("[Thread ${Thread.currentThread().id}] Partitions ${partitions} - Messages - $messages")
            Thread.sleep(1000)
        } catch (e: InterruptedException) {
            e.printStackTrace()
            log.info(e.message)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(this::class.java)
    }
}
