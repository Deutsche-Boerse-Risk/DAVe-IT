package com.deutscheboerse.risk.dave.utils;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

import java.util.Collection;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BrokerFillerCorrectData implements BrokerFiller {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerFillerCorrectData.class);

    private static final String BROKER_USERNAME = "admin";
    private static final String BROKER_PASSWORD = "admin";
    private static final String ACCOUNT_MARGIN_QUEUE = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin";
    private static final String LIQUI_GROUP_MARGIN_QUEUE = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin";
    private static final String LIQUI_GROUP_SPLIT_MARGIN_QUEUE = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupSplitMargin";
    private static final String POOL_MARGIN_QUEUE = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPoolMargin";
    private static final String POSITION_REPORT_QUEUE = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPositionReport";
    private static final String RISK_LIMIT_UTILIZATION_QUEUE = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVERiskLimitUtilization";
    private final Vertx vertx;
    private static ProtonConnection protonConnection;

    public BrokerFillerCorrectData(Vertx vertx) {
        this.vertx = vertx;
    }

    public void setUpAllQueues(int ttSaveNo, Handler<AsyncResult<String>> handler) {
        Future<ProtonConnection> chainFuture = Future.future();
        this.createAmqpConnection()
                .compose(con -> this.populateAccountMarginQueue(con, ttSaveNo))
                .compose(con -> this.populateLiquiGroupMarginQueue(con, ttSaveNo))
                .compose(con -> this.populateLiquiGroupSplitMarginQueue(con, ttSaveNo))
                .compose(con -> this.populatePoolMarginQueue(con, ttSaveNo))
                .compose(con -> this.populatePositionReportQueue(con, ttSaveNo))
                .compose(con -> this.populateRiskLimitUtilizationQueue(con, ttSaveNo))
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(ar -> {
           if (ar.succeeded()) {
               handler.handle(Future.succeededFuture());
           } else {
               handler.handle(Future.failedFuture(ar.cause()));
           }
        });
    }

    public void setUpAccountMarginQueue(int ttSaveNo, Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populateAccountMarginQueue, ttSaveNo, handler);
    }

    public void setUpLiquiGroupMarginQueue(int ttSaveNo, Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populateLiquiGroupMarginQueue, ttSaveNo, handler);
    }

    public void setUpLiquiGroupSplitMarginQueue(int ttSaveNo, Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populateLiquiGroupSplitMarginQueue, ttSaveNo, handler);
    }

    public void setUpPoolMarginQueue(int ttSaveNo, Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populatePoolMarginQueue, ttSaveNo, handler);
    }

    public void setUpPositionReportQueue(int ttSaveNo, Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populatePositionReportQueue, ttSaveNo, handler);
    }

    public void setUpRiskLimitUtilizationQueue(int ttSaveNo, Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populateRiskLimitUtilizationQueue, ttSaveNo, handler);
    }

    private void setUpQueue(BiFunction<ProtonConnection, Integer, Future<ProtonConnection>> populateFunction, int ttSaveNo, Handler<AsyncResult<String>> handler) {
        this.createAmqpConnection()
                .compose(con -> populateFunction.apply(con, ttSaveNo))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        handler.handle(Future.succeededFuture());
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private Future<ProtonConnection> createAmqpConnection() {
        Future<ProtonConnection> createAmqpConnectionFuture = Future.future();
        ProtonClient protonClient = ProtonClient.create(vertx);
        final String host= TestConfig.BROKER_IP;
        final int port = TestConfig.BROKER_PORT;
        protonClient.connect(host, port, BROKER_USERNAME, BROKER_PASSWORD, connectResult -> {
            if (connectResult.succeeded()) {
                connectResult.result().setContainer("dave/marginLoaderIT").openHandler(openResult -> {
                    if (openResult.succeeded()) {
                        LOG.info("Connected to {}:{}", "localhost", port);
                        BrokerFillerCorrectData.protonConnection = openResult.result();
                        createAmqpConnectionFuture.complete(BrokerFillerCorrectData.protonConnection);
                    } else {
                        createAmqpConnectionFuture.fail(openResult.cause());
                    }
                }).open();
            } else {
                createAmqpConnectionFuture.fail(connectResult.cause());
            }
        });
        return createAmqpConnectionFuture;
    }

    private Future<ProtonConnection> populateAccountMarginQueue(ProtonConnection protonConnection, int ttSaveNo) {
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(ttSaveNo, ttSaveNo)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, ACCOUNT_MARGIN_QUEUE, ttsaveNumbers, this::createAccountMarginGPBObjectList);
    }

    private Future<ProtonConnection> populateLiquiGroupMarginQueue(ProtonConnection protonConnection, int ttSaveNo) {
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(ttSaveNo, ttSaveNo)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, LIQUI_GROUP_MARGIN_QUEUE, ttsaveNumbers, this::createLiquiGroupMarginGPBObjectList);
    }

    private Future<ProtonConnection> populateLiquiGroupSplitMarginQueue(ProtonConnection protonConnection, int ttSaveNo) {
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(ttSaveNo, ttSaveNo)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, LIQUI_GROUP_SPLIT_MARGIN_QUEUE, ttsaveNumbers, this::createLiquiGroupSplitMarginGPBObjectList);
    }

    private Future<ProtonConnection> populatePoolMarginQueue(ProtonConnection protonConnection, int ttSaveNo) {
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(ttSaveNo, ttSaveNo)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, POOL_MARGIN_QUEUE, ttsaveNumbers, this::createPoolMarginGPBObjectList);
    }

    private Future<ProtonConnection> populatePositionReportQueue(ProtonConnection protonConnection, int ttSaveNo) {
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(ttSaveNo, ttSaveNo)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, POSITION_REPORT_QUEUE, ttsaveNumbers, this::createPositionReportGPBObjectList);
    }

    private Future<ProtonConnection> populateRiskLimitUtilizationQueue(ProtonConnection protonConnection, int ttSaveNo) {
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(ttSaveNo, ttSaveNo)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, RISK_LIMIT_UTILIZATION_QUEUE, ttsaveNumbers, this::createRiskLimitUtilizationGPBObjectList);
    }

    protected Future<ProtonConnection> populateQueue(ProtonConnection protonConnection, String queueName, Collection<Integer> ttsaveNumbers, Function<Integer, Optional<ObjectList.GPBObjectList>> gpbBuilder) {
        Future<ProtonConnection> populateQueueFuture = Future.future();
        protonConnection.createSender(queueName).openHandler(openResult -> {
            if (openResult.succeeded()) {
                boolean allSent = true;
                ProtonSender sender = openResult.result();
                sender.setAutoSettle(true);
                for (Integer ttsaveNumber: ttsaveNumbers) {
                    Message message = Message.Factory.create();
                    Optional<ObjectList.GPBObjectList> gpbObjectList = gpbBuilder.apply(ttsaveNumber);
                    if (gpbObjectList.isPresent()) {
                        byte[] messageBytes = gpbObjectList.get().toByteArray();
                        message.setBody(new Data(new Binary(messageBytes)));
                        sender.send(message);
                    } else {
                        allSent = false;
                    }
                }
                if (allSent) {
                    LOG.info("All messages sent to {} ", sender.getRemoteTarget().getAddress());
                    populateQueueFuture.complete(protonConnection);
                } else {
                    populateQueueFuture.fail("Failed to send some messages to " + queueName);
                }
            } else {
                populateQueueFuture.fail(openResult.cause());
            }
        }).open();
        return populateQueueFuture;
    }

    private Optional<ObjectList.GPBObjectList> createAccountMarginGPBObjectList(int ttsaveNo) {
        final String folderName = "accountMargin";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.AccountMargin data = DataHelper.createAccountMarginGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.accountMargin, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    private Optional<ObjectList.GPBObjectList> createLiquiGroupMarginGPBObjectList(int ttsaveNo) {
        final String folderName = "liquiGroupMargin";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.LiquiGroupMargin data = DataHelper.createLiquiGroupMarginGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.liquiGroupMargin, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    private Optional<ObjectList.GPBObjectList> createLiquiGroupSplitMarginGPBObjectList(int ttsaveNo) {
        final String folderName = "liquiGroupSplitMargin";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.LiquiGroupSplitMargin data = DataHelper.createLiquiGroupSplitMarginGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.liquiGroupSplitMargin, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    private Optional<ObjectList.GPBObjectList> createPoolMarginGPBObjectList(int ttsaveNo) {
        final String folderName = "poolMargin";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.PoolMargin data = DataHelper.createPoolMarginGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.poolMargin, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    private Optional<ObjectList.GPBObjectList> createPositionReportGPBObjectList(int ttsaveNo) {
        final String folderName = "positionReport";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.PositionReport data = DataHelper.createPositionReportGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.positionReport, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    private Optional<ObjectList.GPBObjectList> createRiskLimitUtilizationGPBObjectList(int ttsaveNo) {
        final String folderName = "riskLimitUtilization";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.RiskLimitUtilization data = DataHelper.createRiskLimitUtilizationGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.riskLimitUtilization, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    protected Optional<ObjectList.GPBObjectList> createGPBFromJson(String folderName, int ttsaveNo, Function<JsonObject, ObjectList.GPBObject> creator) {
        ObjectList.GPBObjectList.Builder gpbObjectListBuilder = ObjectList.GPBObjectList.newBuilder();
        JsonObject lastRecord = new JsonObject();
        DataHelper.readTTSaveFile(folderName, ttsaveNo).forEach(json -> {
            ObjectList.GPBObject gpbObject = creator.apply(json);
            gpbObjectListBuilder.addItem(gpbObject);
            lastRecord.mergeIn(json);
        });
        if (lastRecord.isEmpty()) {
            return Optional.empty();
        }
        ObjectList.GPBHeader gpbHeader = ObjectList.GPBHeader.newBuilder()
                .setExtension(PrismaReports.prismaHeader, DataHelper.createPrismaHeaderFromJson(lastRecord)).build();
        gpbObjectListBuilder.setHeader(gpbHeader);
        return Optional.of(gpbObjectListBuilder.build());
    }
}
