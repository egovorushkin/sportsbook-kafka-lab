package dev.egovorushkin.odds_feed_vertx;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.VerticleBase;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;

import java.util.HashMap;
import java.util.Map;

public class MainVerticle extends VerticleBase {
  private static final String SPORTSBOOK_ODDS_UPDATE_TOPIC = "sportsbook.odds.updates";
  private KafkaProducer<String, String> producer;

  Map<Long, Promise<Void>> pendingRetries = new HashMap<>();
  private boolean stopping;

  @Override
  public Future<?> start() {
    producer = KafkaProducer.create(vertx, KafkaProducerConfig.create());

    return vertx.createHttpServer().requestHandler(req -> {
      req.response()
          .putHeader("content-type", "text/plain")
          .end("Hello from Vert.x!");
    }).listen(8888).onSuccess(http -> {
      System.out.println("HTTP server started on port 8888");
      var matchA1 = KafkaProducerRecord.create(SPORTSBOOK_ODDS_UPDATE_TOPIC, "match-011", "odds: 0.5");
      var matchA2 = KafkaProducerRecord.create(SPORTSBOOK_ODDS_UPDATE_TOPIC, "match-011", "odds: 0.6");
      var matchB1 = KafkaProducerRecord.create(SPORTSBOOK_ODDS_UPDATE_TOPIC, "match-022", "odds: 0.7");
      var matchB2 = KafkaProducerRecord.create(SPORTSBOOK_ODDS_UPDATE_TOPIC, "match-022", "odds: 0.9");

      // producer.send(matchA1)
      // // Future.<Void>failedFuture("simulated send failure") // simulate failure
      // for
      // // match A1
      // .compose(ignored -> producer.send(matchA2))
      // .onFailure(err -> {
      // System.err.println("Match A failed: " + err);

      // });

      sendWithRetry(matchA1, 3)
          .compose(ignored -> sendWithRetry(matchA2, 3))
          .onFailure(err -> System.err.println("Match A failed: " + err));

      // producer.send(matchB1)
      // .compose(ignored -> producer.send(matchB2))
      // .onFailure(err -> System.err.println("Match B failed: " + err));

      sendWithRetry(matchB1, 3)
          .compose(ignored -> sendWithRetry(matchB2, 3))
          .onFailure(err -> System.err.println("Match matchB1 failed: " + err));
    });
  }

  private Future<RecordMetadata> sendWithRetry(KafkaProducerRecord<String, String> record, int retries) {
    if (stopping) {
      return Future.failedFuture("Producer is stopping");
    }

    return producer.send(record).recover(err -> {
      System.err.println("Send failed: " + err);
      if (stopping || !(err instanceof RetriableException) || retries <= 0) {
        return Future.failedFuture(err);
      }

      Promise<Void> delay = Promise.promise();
      long timerId = vertx.setTimer(1000, id -> {
        pendingRetries.remove(id);
        delay.tryComplete();
      });
      pendingRetries.put(timerId, delay);
      return delay.future().compose(ignored -> sendWithRetry(record, retries - 1));
    });
  }

  @Override
  public Future<?> stop() throws Exception {
    stopping = true;

    Map<Long, Promise<Void>> copyPendingRetries = Map.copyOf(pendingRetries);
    pendingRetries = new HashMap<>();

    for (Map.Entry<Long, Promise<Void>> pendingRetry : copyPendingRetries.entrySet()) {
      vertx.cancelTimer(pendingRetry.getKey());
      pendingRetry.getValue().fail("Fail retry due to stop verticle");
    }

    return producer.close();
  }

  private static class KafkaProducerConfig {
    private static Map<String, String> create() {
      Map<String, String> config = new HashMap<>();
      config.put("bootstrap.servers", "localhost:19092");
      config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      config.put("acks", "all");
      config.put("enable.idempotence", "true");
      // Timeout settings for producer for testing purposes
      // config.put("max.block.ms", "5000");
      // config.put("request.timeout.ms", "5000");
      // config.put("delivery.timeout.ms", "10000");
      return config;
    }
  }
}
