package me.kafkaparallelconsumer.listener

import KafkaParallelListener
import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import io.confluent.parallelconsumer.PollContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.stereotype.Component
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method

@Component
class KafkaParallelListenerProcessor(
    private val parallelKafkaConsumer: ConsumerFactory<String, Any>,
) : BeanPostProcessor, DisposableBean {

    private val kafkaParallelConsumerFactory: KafkaParallelConsumerFactory<String, Any> =
        KafkaParallelConsumerFactory()

    private val consumers = mutableListOf<ParallelStreamProcessor<String, Any>>()

    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any? {
        bean.javaClass.methods.forEach { method: Method ->
            method.getAnnotation(KafkaParallelListener::class.java)?.let { annotation: KafkaParallelListener ->
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
        val parallelConsumerProcessingOrder: ParallelConsumerOptions.ProcessingOrder =
            convertToParallelConsumerProcessingOrder(kafkaParallelListener.ordering)

        val consumerProcessor: ParallelStreamProcessor<String, Any> =
            kafkaParallelConsumerFactory.createConsumerProcessor(
                kafkaConsumerFactory = parallelKafkaConsumer,
                topics = kafkaParallelListener.topics,
                ordering = parallelConsumerProcessingOrder,
                maxConcurrency = kafkaParallelListener.concurrency,
                groupId = kafkaParallelListener.groupId,
                clientIdPrefix = kafkaParallelListener.clientIdPrefix,
                clientIdSuffix = "",
            )

        consumerProcessor.poll { context: PollContext<String, Any> ->
            try {
                val record = context.singleConsumerRecord
                method.invoke(bean, record)
            } catch (e: InvocationTargetException) {
                // TODO: error handler 를 추가할 수 있도록 구현 필요
                val originalThrowable: Throwable = e.targetException
                log.error("Kafka parallel consumer error occurred...", originalThrowable)
            } catch (throwable: Throwable) {
                // TODO: error handler 를 추가할 수 있도록 구현 필요
                log.error("Kafka parallel consumer error occurred...", throwable)
            }
        }

        consumers.add(consumerProcessor)
    }

    private fun convertToParallelConsumerProcessingOrder(ordering: ParallelConsumerOptions.ProcessingOrder): ParallelConsumerOptions.ProcessingOrder {
        val parallelConsumerProcessingOrder: ParallelConsumerOptions.ProcessingOrder = when (ordering) {
            ParallelConsumerOptions.ProcessingOrder.KEY -> ParallelConsumerOptions.ProcessingOrder.KEY
            ParallelConsumerOptions.ProcessingOrder.PARTITION -> ParallelConsumerOptions.ProcessingOrder.PARTITION
            ParallelConsumerOptions.ProcessingOrder.UNORDERED -> ParallelConsumerOptions.ProcessingOrder.UNORDERED
        }
        return parallelConsumerProcessingOrder
    }

    override fun destroy() {
        log.info("Kafka parallel consumers closed...")
        consumers.forEach { parallelStreamProcessor: ParallelStreamProcessor<String, Any> ->
            try {
                parallelStreamProcessor.close()
            } catch (e: Exception) {
                log.error("Kafka parallel consumer close fail...", e)
            }
        }
        log.info("Kafka parallel consumers close completed...")
    }

    companion object {
        private val log = LoggerFactory.getLogger(this::class.java)
    }
}
