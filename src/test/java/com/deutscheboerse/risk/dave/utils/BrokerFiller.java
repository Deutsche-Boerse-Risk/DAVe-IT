package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface BrokerFiller {
    void setUpAllQueues(int ttSaveNo, Handler<AsyncResult<String>> handler);
    void setUpAccountMarginQueue(int ttSaveNo, Handler<AsyncResult<String>> handler);
    void setUpLiquiGroupMarginQueue(int ttSaveNo, Handler<AsyncResult<String>> handler);
    void setUpLiquiGroupSplitMarginQueue(int ttSaveNo, Handler<AsyncResult<String>> handler);
    void setUpPoolMarginQueue(int ttSaveNo, Handler<AsyncResult<String>> handler);
    void setUpPositionReportQueue(int ttSaveNo, Handler<AsyncResult<String>> handler);
    void setUpRiskLimitUtilizationQueue(int ttSaveNo, Handler<AsyncResult<String>> handler);
}
