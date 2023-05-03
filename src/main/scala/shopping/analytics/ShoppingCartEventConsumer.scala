package shopping.analytics

import akka.Done
import akka.actor.typed.ActorSystem
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.RestartSettings
import akka.stream.scaladsl.RestartSource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import com.google.protobuf.any.{Any => ScalaPBAny}
import shopping.cart.proto

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object ShoppingCartEventConsumer {

  private val log = LoggerFactory.getLogger("shopping.analytics.ShoppingCartEventConsumer")

  def init(system: ActorSystem[_]): Unit = {
    implicit val sys: ActorSystem[_] = system
    implicit val ec: ExecutionContext = system.executionContext

    val topic = system.settings.config.getString("shopping-analytics-service.kafka.topic")
    val consumerSettings = ConsumerSettings(
      system,
      new StringDeserializer,
      new ByteArrayDeserializer).withGroupId("shopping-cart-analytics")
    val committerSettings = CommitterSettings(system)

    RestartSource
      .onFailuresWithBackoff(
        RestartSettings(
          minBackoff = 1.second,
          maxBackoff = 30.seconds,
          randomFactor = 0.1
        )) { () =>
        Consumer.committableSource(
          consumerSettings,
          Subscriptions.topics(topic)
        )
          .mapAsync(1) { msg =>
            handleRecord(msg.record).map(_ => msg.committableOffset)
          }
          .via(Committer.flow(committerSettings))
      }
      .run()
  }

  private def handleRecord(record: ConsumerRecord[String, Array[Byte]]): Future[Done] = {
    val x = ScalaPBAny.parseFrom(record.value())
    val typeUrl = x.typeUrl
    try {
      val inputBytes = x.value.newCodedInput()
      val event =
        typeUrl match {
          case "shopping-cart-service/shoppingcart.ItemAdded" =>
            proto.ItemAdded.parseFrom(inputBytes)
          case "shopping-cart-service/shoppingcart.ItemRemoved" =>
              proto.ItemRemoved.parseFrom(inputBytes)
          case "shopping-cart-service/shoppingcart.ItemQuantityAdjusted" =>
              proto.ItemQuantityAdjusted.parseFrom(inputBytes)
          case "shopping-cart-service/shoppingcart.CheckedOut" =>
              proto.CheckedOut.parseFrom(inputBytes)
          case other =>
            throw new IllegalArgumentException(s"Unexpected type URL $other")
        }

      event match {
        case proto.ItemAdded(cartId, itemId, quantity, _) =>
          log.info(s"ItemAdded: cartId=$cartId, itemId=$itemId, quantity=$quantity")
        case proto.ItemRemoved(cartId, itemId, _) =>
          log.info(s"ItemRemoved: cartId=$cartId, itemId=$itemId")
        case proto.ItemQuantityAdjusted(cartId, itemId, quantity, _) =>
          log.info(s"ItemQuantityAdjusted: cartId=$cartId, itemId=$itemId, quantity=$quantity")
        case proto.CheckedOut(cartId, _) =>
          log.info(s"CheckedOut: cartId=$cartId")
      }

      Future.successful(Done)
    } catch {
      case NonFatal(e) =>
        log.error("Could not process event of type [{}]", typeUrl, e)
        Future.successful(Done)
    }
  }
}
