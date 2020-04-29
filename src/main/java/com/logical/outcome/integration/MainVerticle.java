package com.logical.outcome.integration;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.logical.outcome.integration.utils.Runner;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {
  private WebClient client;
  private JsonArray foundOrganisations;

  public static void main(String[] args) {
    Runner.runExample(MainVerticle.class);
  }

  @Override
  public void start(){

    client = WebClient.create(vertx);

    Scheduler scheduler = RxHelper.scheduler(vertx);

    Flowable<Long> o = Flowable.interval(10, TimeUnit.SECONDS, scheduler);

    o.subscribe(item -> {
      get(client)
        .flatMap(organisations -> {
          JsonArray apiOrganisationUnits = organisations.body().getJsonArray("organisationUnits");
          foundOrganisations = apiOrganisationUnits;
          String countries = apiOrganisationUnits.stream().map(text -> {
            JsonObject xx = (JsonObject) text;
            return "C" + xx.getInteger("Parent Id");
          }).distinct().collect(Collectors.joining(","));
          String teams = apiOrganisationUnits.stream().map(text -> {
            JsonObject xx = (JsonObject) text;
            return "T" + xx.getInteger("Team ID");
          }).distinct().collect(Collectors.joining(","));
          String codes = "[" + countries + "," + teams + "]";
          Multimap<String, String> queryParams =
            ImmutableMultimap.<String, String>builder()
              .put("paging", "false")
              .put("fields", "id,name,code,openingDate")
              .put("filter", String.format("code:in:%s", codes))
              .build();
          return get(client, "admin", "district", queryParams);
        }).flatMap(ous -> {
        JsonArray dhis2OrganisationUnits = ous.body().getJsonArray("organisationUnits");
        Map<String, JsonObject> code = dhis2OrganisationUnits.stream().collect(Collectors.toMap(x -> {
          JsonObject xx = (JsonObject) x;
          return xx.getString("code");
        }, x -> (JsonObject) x));
        JsonArray newUnits = new JsonArray();
        this.foundOrganisations.stream().forEach(unit -> {
          JsonObject currentUnit = (JsonObject) unit;
          String teamId = "T" + currentUnit.getInteger("Team ID");
          String countryId = "C" + currentUnit.getInteger("Parent Id");
          if (!code.containsKey(teamId) && code.containsKey(countryId)) {
            JsonObject newUnit = new JsonObject();
            newUnit.put("code", teamId);
            newUnit.put("name", currentUnit.getString("Team Name"));
            newUnit.put("shortName", currentUnit.getString("Team Name"));
            newUnit.put("description", currentUnit.getString("Team Type"));
            newUnit.put("openingDate", code.get(countryId).getString("openingDate"));

            JsonObject parent = new JsonObject();
            parent.put("id", code.get(countryId).getString("id"));
            newUnit.put("parent", parent);
            newUnits.add(newUnit);
          }
        });
        JsonObject payload = new JsonObject();
        payload.put("organisationUnits", newUnits);
        return post(client, "admin", "district", null, payload);
      }).subscribe(units -> {
        System.out.println(units.body().getString("status"));
        System.out.println(units.body().getJsonObject("stats").encodePrettily());
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
    HttpRequest<Buffer> request = ctx.postAbs("http://localhost:8080/api/metadata")
      .basicAuthentication(username, password)
      .putHeader("Accept", "application/json");
    return getHttpResponseSingle(params, data, request);
  }

  private Single<HttpResponse<JsonObject>> get(
    WebClient ctx
  ) {
    HttpRequest<Buffer> request = ctx.getAbs("http://localhost:3002/organisations")
      .putHeader("Accept", "application/json");
    return request.as(BodyCodec.jsonObject())
      .rxSend();
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

//  private JsonObject searchByCode(){
//
//  }

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
