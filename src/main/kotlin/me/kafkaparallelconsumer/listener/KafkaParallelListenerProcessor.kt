package me.kafkaparallelconsumer.listener

import io.confluent.parallelconsumer.ParallelStreamProcessor
import io.confluent.parallelconsumer.PollContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.stereotype.Component
import java.lang.reflect.Method

@Component
class KafkaParallelListenerProcessor(
    private val kafkaConsumerFactory: ConsumerFactory<String, Any>,
) : BeanPostProcessor, DisposableBean {

    private val kafkaParallelConsumerFactory = KafkaParallelConsumerFactory<String, Any>()
    private val consumers = mutableListOf<ParallelStreamProcessor<String, Any>>()

    override fun postProcessAfterInitialization(
        bean: Any,
        beanName: String,
    ): Any? {
        bean.javaClass.methods.forEach { method ->
            method.getAnnotation(KafkaParallelListener::class.java)?.let { annotation ->
                processKafkaParallelListenerMethod(bean, method, annotation)
            }
        }
        return bean
    }

    private fun processKafkaParallelListenerMethod(
        bean: Any,
        method: Method,
        kafkaParallelListener: KafkaParallelListener,
    ) {
        val processor = kafkaParallelConsumerFactory.createConsumerProcessor(
            kafkaConsumerFactory = kafkaConsumerFactory,
            topics = kafkaParallelListener.topics,
            ordering = kafkaParallelListener.ordering,
            maxConcurrency = kafkaParallelListener.concurrency,
            groupId = kafkaParallelListener.groupId,
            clientIdPrefix = kafkaParallelListener.clientIdPrefix,
            clientIdSuffix = "",
        )

        processor.poll { context: PollContext<String, Any> ->
            try {
                val record = context.singleConsumerRecord
                method.invoke(bean, record)
            } catch (e: Exception) {
                log.error("Error while processing message", e)
            }
        }

        consumers.add(processor)
    }

    override fun destroy() {
        log.info("Closing all Kafka Parallel Consumers...")
        consumers.forEach { it.close() }
    }

    companion object {
        private val log = LoggerFactory.getLogger(this::class.java)
    }
}
