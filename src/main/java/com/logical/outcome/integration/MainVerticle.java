package com.logical.outcome.integration;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.logical.outcome.integration.utils.Runner;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.RxHelper;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.client.HttpRequest;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {
  private WebClient client;

  public static void main(String[] args) {
    Runner.runExample(MainVerticle.class);
  }

  @Override
  public void start() throws Exception {

    client = WebClient.create(vertx);

    Scheduler scheduler = RxHelper.scheduler(vertx);

    Flowable<Long> o = Flowable.interval(10, TimeUnit.SECONDS, scheduler);

    o.subscribe(item -> {
      DateTimeFormatter inBuiltFormatter2 = DateTimeFormatter.ISO_DATE_TIME;
      String afterDate = LocalDateTime.now().minusSeconds(10).format(inBuiltFormatter2);
      Multimap<String, String> queryParams =
        ImmutableMultimap.<String, String>builder()
          .put("fields", "id,name,shortName,openingDate,code,parent[id]")
          .put("paging", "false")
          .put("filter", String.format("lastUpdated:ge:%s", afterDate))
          .build();
      get(client, "admin", "district", queryParams)
        .flatMap(organisations -> {
          return post(client, "admin", "district", null, organisations.body());
        })
        .subscribe(units -> {
          System.out.println(units.body().encodePrettily());
        }, error -> {
          System.out.println(error.getMessage());
        });
    }, err -> {
      System.out.println(err.getMessage());
    });

  }

  private Single<HttpResponse<JsonObject>> post(
    WebClient ctx,
    String username,
    String password,
    Multimap<String, String> params,
    JsonObject data
  ) {
    HttpRequest<Buffer> request = ctx.postAbs("https://play.dhis2.org/2.30/api/metadata")
      .basicAuthentication(username, password)
      .putHeader("Accept", "application/json");
    return getHttpResponseSingle(params, data, request);
  }

  private Single<HttpResponse<JsonObject>> get(
    WebClient ctx,
    String username,
    String password,
    Multimap<String, String> params
  ) {
    HttpRequest<Buffer> request = ctx.getAbs("http://localhost:8080/api/organisationUnits.json")
      .basicAuthentication(username, password)
      .putHeader("Accept", "application/json");
    if (params != null && params.size() > 0) {
      for (Map.Entry<String, String> entry : params.entries()) {
        request.addQueryParam(entry.getKey(), entry.getValue());
      }
    }
    return request.as(BodyCodec.jsonObject())
      .rxSend();
  }

  private Single<HttpResponse<JsonObject>> getHttpResponseSingle(Multimap<String, String> params, JsonObject data, HttpRequest<Buffer> request) {
    if (params != null && params.size() > 0) {
      for (Map.Entry<String, String> entry : params.entries()) {
        request.addQueryParam(entry.getKey(), entry.getValue());
      }
    }
    return request.as(BodyCodec.jsonObject())
      .rxSendJsonObject(data);
  }

}
