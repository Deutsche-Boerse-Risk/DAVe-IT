package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.net.SelfSignedCertificate;

public class TestConfig {

    public static final int BROKER_PORT = Integer.getInteger("cil.tcpport", 5672);
    public static final SelfSignedCertificate HTTP_SERVER_CERTIFICATE = SelfSignedCertificate.create();
    public static final SelfSignedCertificate HTTP_CLIENT_CERTIFICATE = SelfSignedCertificate.create();
    public static final int DAVE_API_HEALTHCHECK_PORT = Integer.getInteger("api.healthcheck.port", 8080);;
    public static final int DAVE_STOREMANAGER_HEALTHCHECK_PORT = Integer.getInteger("storemanager.healthcheck.port", 8080);;
    public static final int DAVE_MARGINLOADER_HEALTHCHECK_PORT = Integer.getInteger("marginloader.healthcheck.port", 8080);;

}
