/**
 * This is a fully working example of Twitter's Streaming API client.
 * NOTE: this may stop working if at any point Twitter does some breaking changes to this API or the JSON structure.
 */

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model._
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.serialization.SerializationExtension
import akka.stream.{ActorAttributes, ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.scaladsl.{Flow, Framing, Sink, Source}
import akka.util.ByteString
import com.hunorkovacs.koauth.domain.KoauthRequest
import com.hunorkovacs.koauth.service.consumer.DefaultConsumerService
import com.sksamuel.avro4s.{AvroOutputStream, AvroSchema, SchemaFor}
import com.typesafe.config.ConfigFactory
import org.apache.avro.reflect.AvroEncode
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

object TwitterStreamer extends App {

	val conf = ConfigFactory.load()
	val secrets = ConfigFactory.load()

	//Get your credentials from https://apps.twitter.com and replace the values below
	private val consumerKey = conf.getString("twitter.consumerKey")
	private val consumerSecret = conf.getString("twitter.consumerSecret")
	private val accessToken = conf.getString("twitter.accessToken")
	private val accessTokenSecret = conf.getString("twitter.accessTokenSecret")
	private val url = "https://stream.twitter.com/1.1/statuses/filter.json"

	implicit val system = ActorSystem()
	implicit val materializer = ActorMaterializer()
	implicit val formats = DefaultFormats
//	implicit val ec= system.dispatcher

	private val consumer = new DefaultConsumerService(system.dispatcher)
	case class test(name: String, age: Int)
  val schema = AvroSchema[Tweet]
  val serial = SerializationExtension(system).findSerializerFor(Tweet)

	val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
		.withBootstrapServers("127.0.0.1:9092")
//	val producer = producerSettings.createKafkaProducer()
	// producer.send(new ProducerRecord[Array[Byte], String]("twittur", "you know what? fuck you."))
//  val flow: Flow[ByteString, Tweet, NotUsed] = Flow[ByteString].map(bs =>
//    .scan("")((acc, curr) => if (acc.contains("\r\n")) curr.utf8String else acc + curr.utf8String)
//      .filter(_.contains("\r\n"))
//      .map(json => Try(parse(json).extract[Tweet]))
//  )
  val sink: Sink[Any, Future[Done]] = Sink.foreach(println)

	//Filter tweets by a term "london"
	val body = "track=london"
	val uri_source = Uri(url)

	//Create Oauth 1a header
	val oauthHeader: Future[String] = consumer.createOauthenticatedRequest(
		KoauthRequest(
			method = "POST",
			url = url,
			authorizationHeader = None,
			body = Some(body)
		),
		consumerKey,
		consumerSecret,
		accessToken,
		accessTokenSecret
	) map (_.header)

	oauthHeader.onComplete {
		case Success(header) =>
			val httpHeaders: List[HttpHeader] = List(
				HttpHeader.parse("Authorization", header) match {
					case ParsingResult.Ok(h, _) => Some(h)
					case _ => None
				},
				HttpHeader.parse("Accept", "*/*") match {
					case ParsingResult.Ok(h, _) => Some(h)
					case _ => None
				}
			).flatten
			val httpRequest: HttpRequest = HttpRequest(
				method = HttpMethods.POST,
				uri = uri_source,
				headers = httpHeaders,
				entity = FormData(("track", "london")).toEntity
			)



    val chunkBuffer = Framing.delimiter(ByteString("\r\n"), 25000,false)
      .map(_.utf8String)

    import org.json4s.JsonDSL._
    val flow = Flow[String].map{ json =>
      {
        parse(json) transformField {
          case JField("user", u) => ("user_id", compact(render(u \ "id_str")))
      }}.extract[Tweet]
    }.withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))

    val flow_serialize = Flow[Tweet].map{ tw =>
      serial.toBinary(tw)
    }

    val sink = Flow[Tweet]

    println(httpRequest)
    val req = Http().singleRequest(httpRequest)
      req.onComplete {
        case Success(res) if res.status.isSuccess() =>
          println("Success baby yeahhhh")
          println(res.status)
          res.entity.dataBytes
            .via(chunkBuffer)
            .via(flow)
					  .via(flow_serialize)
            .runForeach(x => println(x.length))
        case Success(res) =>
          println("http BAD status")
          println(res.status)
        case Failure(res) =>
          println("We potato'd")
          println(res)
      }


		case Failure(failure) => println(failure.getMessage)
	}

}