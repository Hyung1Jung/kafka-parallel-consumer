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
class KafkaParallelStreamProcessor(
    private val kafkaConsumerFactory: ConsumerFactory<String, Any>,
    private val kafkaParallelListenerValidator: KafkaParallelListenerValidator,
) : BeanPostProcessor, DisposableBean {

    private val kafkaParallelConsumerFactory = KafkaParallelStreamProcessorFactory<String, Any>()
    private val consumers = mutableListOf<ParallelStreamProcessor<String, Any>>()

    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any? {
        bean.javaClass.methods
            .mapNotNull { method -> method.getAnnotation(KafkaParallelListener::class.java)?.let { method to it } }
            .forEach { (method, annotation) -> processKafkaParallelListenerMethod(bean, method, annotation) }
        return bean
    }

    private fun processKafkaParallelListenerMethod(
        bean: Any,
        method: Method,
        kafkaParallelListener: KafkaParallelListener,
    ) {
        kafkaParallelListenerValidator.validate(kafkaParallelListener)

        val processor = with(kafkaParallelListener) {
            kafkaParallelConsumerFactory.createParallelStreamProcessor(
                kafkaConsumerFactory = kafkaConsumerFactory,
                topics = topics,
                ordering = ordering,
                maxConcurrency = maxConcurrency,
                batchSize = batchSize,
                groupId = groupId,
                clientIdPrefix = clientIdPrefix,
                clientIdSuffix = "",
            )
        }
        processor.poll { context: PollContext<String, Any> ->
            try {
                val records = context.consumerRecordsFlattened
                val parameterType = method.parameterTypes.firstOrNull()
                if (parameterType != null && parameterType.isAssignableFrom(List::class.java)) {
                    method.invoke(bean, records)
                    return@poll
                }
                records.forEach { method.invoke(bean, it) }
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
