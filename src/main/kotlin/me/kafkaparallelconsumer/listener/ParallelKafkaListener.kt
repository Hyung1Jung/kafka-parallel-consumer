import io.confluent.parallelconsumer.ParallelConsumerOptions
import org.springframework.messaging.handler.annotation.MessageMapping

@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.ANNOTATION_CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MessageMapping
@MustBeDocumented
annotation class KafkaParallelListener(
    val topics: Array<String>,
    val groupId: String = "",
    val concurrency: Int = 3,
    val ordering: ParallelConsumerOptions.ProcessingOrder = ParallelConsumerOptions.ProcessingOrder.KEY,
    val clientIdPrefix: String = "",
)
