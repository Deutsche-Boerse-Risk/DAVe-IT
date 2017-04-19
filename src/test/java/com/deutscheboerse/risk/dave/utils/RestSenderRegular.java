package com.deutscheboerse.risk.dave.utils;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PemTrustOptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RestSenderRegular implements RestSender {
    private static final Logger LOG = LoggerFactory.getLogger(RestSenderRegular.class);

    private static final String STORE_ACCOUNT_MARGIN_API = "api/v1.0/am/latest";
    private static final String STORE_LIQUI_GROUP_MARGIN_API = "api/v1.0/lgm/latest";
    private static final String STORE_LIQUI_GROUP_SPLIT_MARGIN_API = "api/v1.0/lgsm/latest";
    private static final String STORE_POOL_MARGIN_API = "api/v1.0/pm/latest";
    private static final String STORE_POSITION_REPORT_API = "api/v1.0/pr/latest";
    private static final String STORE_RISK_LIMIT_UTILIZATION_API = "api/v1.0/rlu/latest";

    private static final String API_CERT = "-----BEGIN CERTIFICATE-----\\nMIIDuTCCAqGgAwIBAgIUX/6T0GVpUSBfBC6fIIx9s3Sqh/YwDQYJKoZIhvcNAQEL\\nBQAwTDELMAkGA1UEBhMCREUxETAPBgNVBAcTCEVzY2hib3JuMRswGQYDVQQKExJE\\nZXV0c2NoZSBCb2Vyc2UgQUcxDTALBgNVBAsTBFJpc2swHhcNMTcwNDA3MTQyNjAw\\nWhcNMTgwNDA3MTQyNjAwWjBgMQswCQYDVQQGEwJERTERMA8GA1UEBxMIRXNjaGJv\\ncm4xGzAZBgNVBAoTEkRldXRzY2hlIEJvZXJzZSBBRzENMAsGA1UECxMEUmlzazES\\nMBAGA1UEAxMJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC\\nAQEA2CgfpXvZ8hliPym1I2wkJFiqe49c6TBPiLOPUiBuYaIagWBUa5B4yJECutv/\\nj8xyxkRw025OINhOh+YA5ZMLTJxtNM9jQY+dUObFDhkvdfS1Ri1GMXaJTi3f2Dps\\nt9h14KuIeynNyxUB5v0+QkjhGszB4Fnhcm6/1P0IMoPsC1GndlvrsZQAEtcI0pIj\\nNg2SOEK63O8UsLOXladwg2SOoqnG7a0J2CoHKIyl8oJrJ2SbVED/Mnka2r2sWdmS\\n6F4LuXk+9gtecU92UuxUVm+Ota7fgIwaRG3n+wAH29J5dokdu/zVhjUgbZoj4A5N\\n8LOPwJdyYoEmvbWZY1KQeO2JOwIDAQABo38wfTAOBgNVHQ8BAf8EBAMCBaAwHQYD\\nVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHQYDVR0O\\nBBYEFNg2Lrf74uA4sFf+tPLvdjBqJs+FMB8GA1UdIwQYMBaAFC0Ucxnya/ds+SY/\\nSRZhHLhgMazGMA0GCSqGSIb3DQEBCwUAA4IBAQAq4T6t9/fb7ytOaCxWo9uzlX1M\\nhvAhAQce5nVOckNovQN2KgKVqSpN2Vejrf49opsBxSaaoU+JiuI155kF2fjBpJwH\\nopcPYIZC1FG9xC2os9iXurrhHCIf8T4Hzq+rudSk2nM0Oca9/dxd+/a3trosliMr\\nk1cYj7+6mpPJv4qOUbZ7xPnK8PbDmYJYZtQg7ErZY5U5HvHFHhS3tPZofB77P7tW\\n+me/2ouu00uu2bcwmPgkjAL6OG2O53ijkkkO2l5ZCV2DmS5JAldpnbU0V4LqHeEx\\nld6tRx7sW78JURmQC2sFrmmwi3O8/bsUnxQdPkuhHJXQFirRoOYwSJefigu0\\n-----END CERTIFICATE-----";

    private final Vertx vertx;
    final HttpClient httpClient;

    public RestSenderRegular(Vertx vertx) {
        this.vertx = vertx;
        PemTrustOptions pemTrustOptions = new PemTrustOptions().addCertValue(Buffer.buffer(API_CERT));
        HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true)
                .setVerifyHost(true)
                .setPemTrustOptions(pemTrustOptions);
        this.httpClient = this.vertx.createHttpClient(httpClientOptions);
    }

    public void sendAllData(Handler<AsyncResult<Void>> handler) {
        List<Future> futures = new ArrayList<>();
        futures.add(this.sendData(STORE_ACCOUNT_MARGIN_API, DataHelper.ACCOUNT_MARGIN_FOLDER));
        futures.add(this.sendData(STORE_LIQUI_GROUP_MARGIN_API, DataHelper.LIQUI_GROUP_MARGIN_FOLDER));
        futures.add(this.sendData(STORE_LIQUI_GROUP_SPLIT_MARGIN_API, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER));
        futures.add(this.sendData(STORE_POOL_MARGIN_API, DataHelper.POOL_MARGIN_FOLDER));
        futures.add(this.sendData(STORE_POSITION_REPORT_API, DataHelper.POSITION_REPORT_FOLDER));
        futures.add(this.sendData(STORE_RISK_LIMIT_UTILIZATION_API, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER));
        CompositeFuture.all(futures).setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void sendAccountMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_ACCOUNT_MARGIN_API, DataHelper.ACCOUNT_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendLiquiGroupMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_LIQUI_GROUP_MARGIN_API, DataHelper.LIQUI_GROUP_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendLiquiGroupSplitMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_LIQUI_GROUP_SPLIT_MARGIN_API, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendPoolMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_POOL_MARGIN_API, DataHelper.POOL_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendPositionReportData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_POSITION_REPORT_API, DataHelper.POSITION_REPORT_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendRiskLimitUtilizationData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_RISK_LIMIT_UTILIZATION_API, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    private Handler<AsyncResult<Void>> getResponseHandler(Handler<AsyncResult<Void>> handler) {
        return ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        };
    }

    private Future<Void> sendData(String requestURI, String folderName) {
        Future<Void> resultFuture = Future.future();
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(1, 2)
                .boxed()
                .collect(Collectors.toList());
        vertx.executeBlocking(future -> {
            CountDownLatch countDownLatch = new CountDownLatch(ttsaveNumbers.size());
            ttsaveNumbers.forEach(ttsaveNo -> this.sendModels(requestURI, folderName, ttsaveNo, res -> {
                if (res.succeeded()) {
                    countDownLatch.countDown();
                }
            }));
            try {
                countDownLatch.await(30, TimeUnit.SECONDS);
                future.complete();
            } catch (InterruptedException e) {
                future.fail(e.getCause());
            }
        }, resultFuture);
        return resultFuture;
    }

    private void sendModels(String requestURI, String folderName, int ttsaveNo, Handler<AsyncResult<Void>> resultHandler) {
        CountDownLatch countDownLatch = new CountDownLatch(DataHelper.getJsonObjectCount(folderName, ttsaveNo));
        DataHelper.readTTSaveFile(folderName, ttsaveNo, model -> this.postModel(requestURI, model, ar -> {
            if (ar.succeeded()) {
                countDownLatch.countDown();
            }
        }));
        try {
            if (countDownLatch.await(30, TimeUnit.SECONDS)) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture("Unable to send all models"));
            }
        } catch (InterruptedException e) {
            resultHandler.handle(Future.failedFuture("Unable to send all models"));
        }
    }

    protected void postModel(String requestURI, JsonObject model, Handler<AsyncResult<Void>> resultHandler) {
        this.httpClient.request(HttpMethod.POST,
                TestConfig.DAVE_API_HTTP_PORT,
                "localhost",
                requestURI,
                response -> {
                    if (HttpResponseStatus.CREATED.code() == response.statusCode()) {
                        response.bodyHandler(body -> resultHandler.handle(Future.succeededFuture()));
                    } else {
                        LOG.error("Post failed: {}", response.statusMessage());
                        resultHandler.handle(Future.failedFuture(response.statusMessage()));
                    }
                })
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .end(model.encode());
    }
}
