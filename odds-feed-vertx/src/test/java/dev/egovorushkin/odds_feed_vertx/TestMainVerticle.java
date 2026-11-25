package dev.egovorushkin.odds_feed_vertx;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(VertxExtension.class)
public class TestMainVerticle {
  private static final String TOPIC = "sportsbook.odds.updates";
  private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0")
      .asCompatibleSubstituteFor("confluentinc/cp-kafka"));

  @BeforeAll
  static void startKafka() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is unavailable");
    KAFKA.start();
    try (AdminClient admin = AdminClient.create(Map.of(
        "bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin.createTopics(java.util.List.of(
          new NewTopic(TOPIC, 3, (short) 1))).all().get();
    }
  }

  @AfterAll
  static void stopKafka() {
    KAFKA.stop();
  }

  @Test
  void same_match_updates_keep_partition_and_send_order(
      Vertx vertx, VertxTestContext testContext) {
    var consumerConfig = new HashMap<String, String>();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "milestone-1-order-test");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    var consumer = KafkaConsumer.<String, String>create(vertx, consumerConfig);

    var producerConfig = new HashMap<String, String>();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    var producer = KafkaProducer.<String, String>create(vertx, producerConfig);

    var records = new CopyOnWriteArrayList<KafkaConsumerRecord<String, String>>();
    consumer.handler(recordItem -> {
      if ("match-001".equals(recordItem.key())) {
        records.add(recordItem);
      }
      if (records.size() == 3) {
        testContext.verify(() -> {
          org.junit.jupiter.api.Assertions.assertTrue(
              records.stream().allMatch(record -> record.partition() == records.getFirst().partition()));
          org.junit.jupiter.api.Assertions.assertEquals(
              java.util.List.of("odds: 0.5", "odds: 0.6", "odds: 0.7"),
              records.stream().map(KafkaConsumerRecord::value).toList());
          org.junit.jupiter.api.Assertions.assertTrue(
              records.get(0).offset() < records.get(1).offset()
                  && records.get(1).offset() < records.get(2).offset());
        });
        consumer.close();
        producer.close();
        testContext.completeNow();
      }
    });

    consumer.subscribe(TOPIC)
        .compose(ignored -> producer.send(KafkaProducerRecord.create(
            TOPIC, "match-001", "odds: 0.5")))
        .compose(ignored -> producer.send(KafkaProducerRecord.create(
            TOPIC, "match-001", "odds: 0.6")))
        .compose(ignored -> producer.send(KafkaProducerRecord.create(
            TOPIC, "match-001", "odds: 0.7")))
        .onFailure(testContext::failNow);
  }

  @Test
  void shutdown(Vertx vertx, VertxTestContext testContext) {
    var verticle = new MainVerticle();
    Promise<Void> delay = Promise.promise();

    long timerId = vertx.setTimer(60_000, id -> delay.tryComplete());
    verticle.pendingRetries.put(timerId, delay);

    var next = delay.future().compose(ignored -> {
      testContext.failNow("Next update must not run");
      return Future.succeededFuture();
    });

    vertx.deployVerticle(verticle)
        .compose(vertx::undeploy)
        .onComplete(testContext.succeeding(ignored -> {
          testContext.verify(() -> assertTrue(delay.future().failed()));
          testContext.verify(() -> assertFalse(vertx.cancelTimer(timerId)));
          next.onComplete(testContext.failing(err -> testContext.completeNow()));
        }));

  }
}
