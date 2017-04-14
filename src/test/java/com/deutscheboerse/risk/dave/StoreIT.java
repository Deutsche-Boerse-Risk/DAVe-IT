package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.utils.BrokerFiller;
import com.deutscheboerse.risk.dave.utils.BrokerFillerCorrectData;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import com.deutscheboerse.risk.dave.utils.TestConfig;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicLong;

@RunWith(VertxUnitRunner.class)
public class StoreIT {
    private static final Logger LOG = LoggerFactory.getLogger(StoreIT.class);

    private Vertx vertx;
    private static int ACCOUNT_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 1);
    private static int LIQUI_GROUP_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1);
    private static int LIQUI_GROUP_SPLIT_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1);
    private static int POOL_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 1);
    private static int POSITION_REPORT_COUNT = DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 1);
    private static int RISK_LIMIT_UTILIZATION_COUNT = DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1);

    @Before
    public void setUp() {
        this.vertx = Vertx.vertx();
    }

    private MongoClient createMongoClient() {
        return MongoClient.createShared(this.vertx, TestConfig.getMongoClientConfig());
    }

    @Test
    public void testStoreCommands(TestContext context) {
        MongoClient mongoClient = this.createMongoClient();
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
        vertx.close(context.asyncAssertSuccess());
    }
}
