package unit.kafka.integration

import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{CoreUtils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, IntegerSerializer}
import org.junit.{After, Before, Test}

/**
  * Test for tracking down a regression in on-disk log size between versions 0.10.1.1 and 0.10.2.1.
  *
  * Sets up a single broker running with a log message format version of 0.8.2. A producer using the
  * Snappy codec is used to send a fixed number of messages of type (Integer, ByteArray) to the
  * broker. After messages have been received, compute the on-disk size of the log for the topic.
  *
  * For 1,000 messages, each of raw size 1kB, the observed on-disk size for each of the releases
  * was:
  *   - 0.10.1.1          - 122,738 bytes
  *   - 0.10.2.1          - 130,737 bytes
  *   - trunk @ e3892c29c - 130,737 bytes
  */
class MessageSizeTest extends ZooKeeperTestHarness {

  val RECORDS = 1000
  val RECORD_SIZE_BYTES = 1000
  val LOG_MESSAGE_FORMAT_VERSION = "0.8.2"
  val COMPRESSION_CODEC = "snappy"

  private val topic = "test-topic"
  private var server: KafkaServer = _
  private var producer: KafkaProducer[Integer, Array[Byte]] = _

  @Before
  override def setUp(): Unit = {
    super.setUp()

    val brokerProps = new Properties()
    brokerProps.put("zookeeper.connect", zkConnect)
    brokerProps.put("listeners", "PLAINTEXT://localhost:9092")
    brokerProps.put("log.dir", TestUtils.tempDir().getAbsolutePath)
    brokerProps.put("log.message.format.version", LOG_MESSAGE_FORMAT_VERSION)

    // Start up brokers
    val config = KafkaConfig.fromProps(brokerProps)
    server = new KafkaServer(config)
    server.startup()

    // Set up a producer
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", classOf[IntegerSerializer].getCanonicalName)
    producerProps.put("value.serializer", classOf[ByteArraySerializer].getCanonicalName)
    producerProps.put("acks", "1")
    producerProps.put("compression.type", COMPRESSION_CODEC)
    producer = new KafkaProducer[Integer, Array[Byte]](producerProps)
  }

  @After
  override def tearDown(): Unit = {
    producer.close()

    server.shutdown()
    server.awaitShutdown()
    CoreUtils.delete(server.config.logDirs)

    super.tearDown()
  }

  @Test
  def test(): Unit = {
    for (_ <- Range(0, RECORDS)) {
      val result = producer.send(recordWithSize(RECORD_SIZE_BYTES))
      result.get()
    }

    println(s"Total log size = $getLogSize bytes")
  }

  private def recordWithSize(sizeBytes: Int): ProducerRecord[Integer, Array[Byte]] = {
    new ProducerRecord(topic, Array.fill[Byte](sizeBytes)(0))
  }

  private def getLogSize: Long = {
    server.logManager
      .allLogs()
      .map(_.dir)
      .flatMap(_.listFiles)
      .filter(file => file.isFile && file.getName.endsWith(".log"))
      .map(file => file.length)
      .sum
  }
}

