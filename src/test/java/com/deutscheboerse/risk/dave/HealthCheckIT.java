package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.utils.TestConfig;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class HealthCheckIT {

    public static final String REST_HEALTHZ = "/healthz";
    public static final String REST_READINESS = "/readiness";

    private static Vertx vertx;
    private JsonObject expectedHealthResponse;
    private JsonObject expectedReadinessResponse;

    public HealthCheckIT() {
        this.expectedHealthResponse = new JsonObject();
        this.expectedHealthResponse.put("checks", new JsonArray().add(new JsonObject()
                .put("id", "healthz")
                .put("status", "UP")))
                .put("outcome", "UP");
        this.expectedReadinessResponse = new JsonObject();
        this.expectedReadinessResponse.put("checks", new JsonArray().add(new JsonObject()
                .put("id", "readiness")
                .put("status", "UP")))
                .put("outcome", "UP");
    }

    @BeforeClass
    public static void setUp(TestContext context) {
        vertx = Vertx.vertx();
    }

    private Handler<HttpClientResponse> assertEqualsHttpHandler(int expectedCode, String expectedText, TestContext context) {
        final Async async = context.async();
        return response -> {
            context.assertEquals(expectedCode, response.statusCode());
            response.bodyHandler(body -> {
                try {
                    context.assertEquals(expectedText, body.toString());
                    async.complete();
                } catch (Exception e) {
                    context.fail(e);
                }
            });
        };
    }

    @Test
    public void testDaveApiHealth(TestContext context) throws InterruptedException {
        vertx.createHttpClient().getNow(TestConfig.DAVE_API_HEALTHCHECK_PORT, TestConfig.DAVE_API_IP, REST_HEALTHZ,
                assertEqualsHttpHandler(HttpResponseStatus.OK.code(), this.expectedHealthResponse.encode(), context));
    }

    @Test
    public void testDaveApiReadinessOk(TestContext context) throws InterruptedException {
        vertx.createHttpClient().getNow(TestConfig.DAVE_API_HEALTHCHECK_PORT, TestConfig.DAVE_API_IP, REST_READINESS,
                assertEqualsHttpHandler(HttpResponseStatus.OK.code(), this.expectedReadinessResponse.encode(), context));
    }

    @Test
    public void testDaveStoreManagerHealth(TestContext context) throws InterruptedException {
        vertx.createHttpClient().getNow(TestConfig.DAVE_STOREMANAGER_HEALTHCHECK_PORT, TestConfig.DAVE_STORE_MANAGER_IP, REST_HEALTHZ,
                assertEqualsHttpHandler(HttpResponseStatus.OK.code(), this.expectedHealthResponse.encode(), context));
    }

    @Test
    public void testDaveStoreManagerReadinessOk(TestContext context) throws InterruptedException {
        vertx.createHttpClient().getNow(TestConfig.DAVE_STOREMANAGER_HEALTHCHECK_PORT, TestConfig.DAVE_STORE_MANAGER_IP, REST_READINESS,
                assertEqualsHttpHandler(HttpResponseStatus.OK.code(), this.expectedReadinessResponse.encode(), context));
    }

    @Test
    public void testDaveMarginLoaderHealth(TestContext context) throws InterruptedException {
        vertx.createHttpClient().getNow(TestConfig.DAVE_MARGINLOADER_HEALTHCHECK_PORT, TestConfig.DAVE_MARGIN_LOADER_IP, REST_HEALTHZ,
                assertEqualsHttpHandler(HttpResponseStatus.OK.code(), this.expectedHealthResponse.encode(), context));
    }

    @Test
    public void testDaveMarginLoaderReadinessOk(TestContext context) throws InterruptedException {
        vertx.createHttpClient().getNow(TestConfig.DAVE_MARGINLOADER_HEALTHCHECK_PORT, TestConfig.DAVE_MARGIN_LOADER_IP, REST_READINESS,
                assertEqualsHttpHandler(HttpResponseStatus.OK.code(), this.expectedReadinessResponse.encode(), context));
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }
}
