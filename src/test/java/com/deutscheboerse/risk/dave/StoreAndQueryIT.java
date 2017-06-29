package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.utils.BrokerFiller;
import com.deutscheboerse.risk.dave.utils.BrokerFillerCorrectData;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import com.deutscheboerse.risk.dave.utils.TestConfig;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.dns.AddressResolverOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import java.util.concurrent.atomic.AtomicLong;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(VertxUnitRunner.class)
public class StoreAndQueryIT {
    private static final Logger LOG = LoggerFactory.getLogger(StoreAndQueryIT.class);

    private static int ACCOUNT_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 1);
    private static int LIQUI_GROUP_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1);
    private static int LIQUI_GROUP_SPLIT_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1);
    private static int POOL_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 1);
    private static int POSITION_REPORT_COUNT = DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 1);
    private static int RISK_LIMIT_UTILIZATION_COUNT = DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1);
    private static String JWT_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjkwMDAvYXV0aC9yZWFsbXMvbWFzdGVyIiwic3ViIjoiYjgwNGU2ZGUtZWUzNy00NjAwLTg2M2EtY2M4NjlkYmQ1MjUyIiwibmJmIjoxNDkxMzI0NzUzLCJleHAiOjE1MjI4NjA3NTMsImlhdCI6MTQ5MTMyNDc1MywianRpIjoiZjFhZGUzYjEtMGI4OC00NWRlLWE5YjctNzY4Y2FjOWFhYWUzIiwidHlwIjoiQmVhcmVyIn0.kOVqtaEDLc176teca-H_pVnCP4JyYmbp2YxAZpc4tsVSUm0F-8lHQKvnNQsSdRxJND5k5yz7nrpPtcu-JtVJBiiqMBHxP4rZ7mR3gwmLBBP-GLuKOS3MSMTKlTex_6YO4v3Po6EN8gWhdcZzpgyz64mSwCL2A5kmpuz5ILc1rnZCSsOD9aaawJjIqEuCRIxLs0dMax6ryDNvomKW8nL_Pb1ECQ4XZOh93Rl_2XFj6BIUv7Q7QlqAR5jDQw13ZVvmuqqhbttuiIWQ2yys36c99WDmB-U-anbQ4Nd0N4PMAnmG5CvTLkjH-FcKtqU890QDwdeKhSUsxPjJoYsexbdiCg";

    private Vertx vertx;
    private MongoClient mongoClient;

    @Before
    public void setUp() {
        VertxOptions vertxOptions = new VertxOptions().setAddressResolverOptions(new AddressResolverOptions()
                .setHostsValue(Buffer.buffer(
                        String.format("127.0.0.1    localhost\n" +
                        String.format("%s   dave.api", TestConfig.DAVE_API_IP))
                )));
        this.vertx = Vertx.vertx(vertxOptions);
        this.mongoClient = this.createMongoClient();
    }

    private MongoClient createMongoClient() {
        return MongoClient.createShared(this.vertx, TestConfig.getMongoClientConfig());
    }

    @Test
    public void testFillDatabase(TestContext context) {
        final BrokerFiller brokerFiller = new BrokerFillerCorrectData(this.vertx);
        brokerFiller.setUpAllQueues(1, context.asyncAssertSuccess());
        this.testCountSnapshotsInCollection(context, mongoClient, "AccountMargin", ACCOUNT_MARGIN_COUNT, 1);
        this.testCountSnapshotsInCollection(context, mongoClient, "LiquiGroupMargin", LIQUI_GROUP_MARGIN_COUNT, 1);
        this.testCountSnapshotsInCollection(context, mongoClient, "LiquiGroupSplitMargin", LIQUI_GROUP_SPLIT_MARGIN_COUNT, 1);
        this.testCountSnapshotsInCollection(context, mongoClient, "PoolMargin", POOL_MARGIN_COUNT, 1);
        this.testCountSnapshotsInCollection(context, mongoClient, "PositionReport", POSITION_REPORT_COUNT, 1);
        this.testCountSnapshotsInCollection(context, mongoClient, "RiskLimitUtilization", RISK_LIMIT_UTILIZATION_COUNT, 1);
        brokerFiller.setUpAllQueues(2, context.asyncAssertSuccess());
        this.testCountSnapshotsInCollection(context, mongoClient, "AccountMargin", ACCOUNT_MARGIN_COUNT, 2);
        this.testCountSnapshotsInCollection(context, mongoClient, "LiquiGroupMargin", LIQUI_GROUP_MARGIN_COUNT, 2);
        this.testCountSnapshotsInCollection(context, mongoClient, "LiquiGroupSplitMargin", LIQUI_GROUP_SPLIT_MARGIN_COUNT, 2);
        this.testCountSnapshotsInCollection(context, mongoClient, "PoolMargin", POOL_MARGIN_COUNT, 2);
        this.testCountSnapshotsInCollection(context, mongoClient, "PositionReport", POSITION_REPORT_COUNT, 2);
        this.testCountSnapshotsInCollection(context, mongoClient, "RiskLimitUtilization", RISK_LIMIT_UTILIZATION_COUNT, 2);
    }

    @Test
    public void testQueryLatest(TestContext context) {
        this.testQueryDatabase(context, "/api/v1.0/am/latest", ACCOUNT_MARGIN_COUNT);
        this.testQueryDatabase(context, "/api/v1.0/lgm/latest", LIQUI_GROUP_MARGIN_COUNT);
        this.testQueryDatabase(context, "/api/v1.0/lgsm/latest", LIQUI_GROUP_SPLIT_MARGIN_COUNT);
        this.testQueryDatabase(context, "/api/v1.0/pm/latest", POOL_MARGIN_COUNT);
        this.testQueryDatabase(context, "/api/v1.0/pr/latest", POSITION_REPORT_COUNT);
        this.testQueryDatabase(context, "/api/v1.0/rlu/latest", RISK_LIMIT_UTILIZATION_COUNT);
    }

    @Test
    public void testQueryLatestWithKey(TestContext context) {
        AccountMarginModel lastAccountMarginModel = DataHelper.getLastModelFromFile(AccountMarginModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/am/latest", lastAccountMarginModel, new JsonArray().add(lastAccountMarginModel));

        LiquiGroupMarginModel lastLiquiGroupMarginModel = DataHelper.getLastModelFromFile(LiquiGroupMarginModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/lgm/latest", lastLiquiGroupMarginModel, new JsonArray().add(lastLiquiGroupMarginModel));

        LiquiGroupSplitMarginModel lastLiquiGroupSplitMarginModel = DataHelper.getLastModelFromFile(LiquiGroupSplitMarginModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/lgsm/latest", lastLiquiGroupSplitMarginModel, new JsonArray().add(lastLiquiGroupSplitMarginModel));

        PoolMarginModel lastPoolMarginModel = DataHelper.getLastModelFromFile(PoolMarginModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/pm/latest", lastPoolMarginModel, new JsonArray().add(lastPoolMarginModel));

        PositionReportModel lastPositionReportModel = DataHelper.getLastModelFromFile(PositionReportModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/pr/latest", lastPositionReportModel, new JsonArray().add(lastPositionReportModel));

        RiskLimitUtilizationModel lastRiskLimitUtilizationModel = DataHelper.getLastModelFromFile(RiskLimitUtilizationModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/rlu/latest", lastRiskLimitUtilizationModel, new JsonArray().add(lastRiskLimitUtilizationModel));
    }

    @Test
    public void testQueryHistoryWithKey(TestContext context) {
        AccountMarginModel firstAccountMarginModel = DataHelper.getLastModelFromFile(AccountMarginModel.class, 1);
        AccountMarginModel secondAccountMarginModel = DataHelper.getLastModelFromFile(AccountMarginModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/am/history", firstAccountMarginModel, new JsonArray().add(firstAccountMarginModel).add(secondAccountMarginModel));

        LiquiGroupMarginModel firstLiquiGroupMarginModel = DataHelper.getLastModelFromFile(LiquiGroupMarginModel.class, 1);
        LiquiGroupMarginModel secondLiquiGroupMarginModel = DataHelper.getLastModelFromFile(LiquiGroupMarginModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/lgm/history", firstLiquiGroupMarginModel, new JsonArray().add(firstLiquiGroupMarginModel).add(secondLiquiGroupMarginModel));

        LiquiGroupSplitMarginModel firstLiquiGroupSplitMarginModel = DataHelper.getLastModelFromFile(LiquiGroupSplitMarginModel.class, 1);
        LiquiGroupSplitMarginModel secondLiquiGroupSplitMarginModel = DataHelper.getLastModelFromFile(LiquiGroupSplitMarginModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/lgsm/history", firstLiquiGroupSplitMarginModel, new JsonArray().add(firstLiquiGroupSplitMarginModel).add(secondLiquiGroupSplitMarginModel));

        PoolMarginModel firstPoolMarginModel = DataHelper.getLastModelFromFile(PoolMarginModel.class, 1);
        PoolMarginModel secondPoolMarginModel = DataHelper.getLastModelFromFile(PoolMarginModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/pm/history", firstPoolMarginModel, new JsonArray().add(firstPoolMarginModel).add(secondPoolMarginModel));

        PositionReportModel firstPositionReportModel = DataHelper.getLastModelFromFile(PositionReportModel.class, 1);
        PositionReportModel secondPositionReportModel = DataHelper.getLastModelFromFile(PositionReportModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/pr/history", firstPositionReportModel, new JsonArray().add(firstPositionReportModel).add(secondPositionReportModel));

        RiskLimitUtilizationModel firstRiskLimitUtilizationModel = DataHelper.getLastModelFromFile(RiskLimitUtilizationModel.class, 1);
        RiskLimitUtilizationModel secondRiskLimitUtilizationModel = DataHelper.getLastModelFromFile(RiskLimitUtilizationModel.class, 2);
        this.testQueryDatabase(context, "/api/v1.0/rlu/history", firstRiskLimitUtilizationModel, new JsonArray().add(firstRiskLimitUtilizationModel).add(secondRiskLimitUtilizationModel));
    }

    private void testQueryDatabase(TestContext context, String restApi, int expectedCount) {
        final Async asyncClient = context.async();
        WebClient.create(vertx, this.getWebClientOptions())
                .get(TestConfig.DAVE_API_HTTP_PORT, "dave.api", restApi)
                //.as(BodyCodec.jsonArray())
                .putHeader("Authorization", "Bearer " + JWT_TOKEN)
                .send(ar -> {
                    if (ar.succeeded()) {
                        HttpResponse res = ar.result();
                        context.assertEquals(HttpResponseStatus.OK.code(), res.statusCode());
                        context.assertEquals(expectedCount, res.bodyAsJsonArray().size());
                        asyncClient.complete();
                    } else {
                        context.fail(ar.cause());
                    }
                });
    }

    private void testQueryDatabase(TestContext context, String restApi, AbstractModel queryObject, JsonArray expectedResult) {
        final Async asyncClient = context.async();
        HttpRequest<Buffer> httpRequest = WebClient.create(vertx, this.getWebClientOptions())
                .get(TestConfig.DAVE_API_HTTP_PORT, "dave.api", restApi);
        DataHelper.getQueryParams(queryObject).forEach(entry -> {
            httpRequest.addQueryParam(entry.getKey(), entry.getValue().toString());
        });
        httpRequest
                .putHeader("Authorization", "Bearer " + JWT_TOKEN)
                .send(ar -> {
                    if (ar.succeeded()) {
                        HttpResponse res = ar.result();
                        context.assertEquals(HttpResponseStatus.OK.code(), res.statusCode());
                        context.assertEquals(expectedResult, res.bodyAsJsonArray());
                        asyncClient.complete();
                    } else {
                        context.fail(ar.cause());
                    }
                });
    }

    private WebClientOptions getWebClientOptions() {
        PemTrustOptions pemTrustOptions = new PemTrustOptions().addCertValue(Buffer.buffer(TestConfig.DAVE_API_CERTIFICATE));
        return new WebClientOptions()
                .setSsl(true)
                .setVerifyHost(true).setPemTrustOptions(pemTrustOptions);
    }

    private void testCountSnapshotsInCollection(TestContext  context, MongoClient mongoClient, String collection, long expectedCount, int snapshotSize) {
        JsonObject query = new JsonObject().put("snapshots", new JsonObject().put("$size", snapshotSize));
        AtomicLong currentCount = new AtomicLong();
        int tries = 0;
        while (currentCount.get() != expectedCount && tries < 60) {
            Async asyncHistoryCount = context.async();
            mongoClient.find(collection, query, ar -> {
                if (ar.succeeded()) {
                    currentCount.set(ar.result().size());
                    if (currentCount.get() == expectedCount && !asyncHistoryCount.isCompleted()) {
                        asyncHistoryCount.complete();
                    }
                } else {
                    context.fail(ar.cause());
                }
            });
            try {
                asyncHistoryCount.await(1000);
            } catch (Exception ignored) {
                asyncHistoryCount.complete();
            }
            tries++;
        }
        context.assertEquals(expectedCount, currentCount.get());
    }

    @After
    public void cleanup(TestContext context) {
        mongoClient.close();
        vertx.close(context.asyncAssertSuccess());
    }
}
