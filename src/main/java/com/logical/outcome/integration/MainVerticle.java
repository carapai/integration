package com.logical.outcome.integration;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
//import io.vertx.ext.web.client.WebClientOptions;

public class MainVerticle extends AbstractVerticle {

  private HttpRequest<JsonObject> request;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    request = WebClient.create(vertx).get(8080, "localhost", "/api/users.json")
      .basicAuthentication("admin", "district")
      .putHeader("Accept", "application/json")
      .addQueryParam("paging", "false")
      .addQueryParam("fields", "*")
      .addQueryParam("filter", "lastUpdated:ge:2015-03-31T13:30")
      .as(BodyCodec.jsonObject());

    vertx.setPeriodic(3000, id -> fetchJoke());
  }

  private void fetchJoke() {
    request.send(asyncResult -> {
      if (asyncResult.succeeded()) {
        System.out.println(asyncResult.result().body()); // (7)
        System.out.println("🤣");
        System.out.println();
      } else {
        System.out.println(asyncResult.cause());
      }
    });
  }
}
