package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.json.JsonObject;

public class TestConfig {

    public static final int MONGO_PORT = Integer.getInteger("mongodb.port", 27017);
    public static final int BROKER_PORT = Integer.getInteger("cil.tcpport", 5672);
    public static final int DAVE_API_HTTP_PORT = Integer.getInteger("api.http.port", 8443);
    public static final int DAVE_API_HEALTHCHECK_PORT = Integer.getInteger("api.healthcheck.port", 8080);
    public static final int DAVE_STOREMANAGER_HEALTHCHECK_PORT = Integer.getInteger("storemanager.healthcheck.port", 8080);
    public static final int DAVE_MARGINLOADER_HEALTHCHECK_PORT = Integer.getInteger("marginloader.healthcheck.port", 8080);

    public static JsonObject getMongoClientConfig() {
        final String DB_NAME = "DAVe";
        return new JsonObject()
                .put("db_name", DB_NAME)
                .put("connection_string", String.format("mongodb://localhost:%s/?waitqueuemultiple=%d", MONGO_PORT, 20000));
    }

}
