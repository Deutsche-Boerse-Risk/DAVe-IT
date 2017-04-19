package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.json.JsonObject;

public class TestConfig {

    public static final int MONGO_PORT = Integer.getInteger("mongodb.port", 27017);
    public static final int BROKER_PORT = Integer.getInteger("cil.tcpport", 5672);
    public static final int DAVE_API_HTTP_PORT = Integer.getInteger("api.http.port", 8443);
    public static final int DAVE_API_HEALTHCHECK_PORT = Integer.getInteger("api.healthcheck.port", 8080);
    public static final int DAVE_STOREMANAGER_HEALTHCHECK_PORT = Integer.getInteger("storemanager.healthcheck.port", 8080);
    public static final int DAVE_MARGINLOADER_HEALTHCHECK_PORT = Integer.getInteger("marginloader.healthcheck.port", 8080);
    public static final String DAVE_API_CERTIFICATE = "-----BEGIN CERTIFICATE-----\nMIIDuTCCAqGgAwIBAgIUM2+o/MSeEI3bkdZeqcWpdRtc9JEwDQYJKoZIhvcNAQEL\nBQAwTDELMAkGA1UEBhMCREUxETAPBgNVBAcTCEVzY2hib3JuMRswGQYDVQQKExJE\nZXV0c2NoZSBCb2Vyc2UgQUcxDTALBgNVBAsTBFJpc2swHhcNMTcwNDA3MTQyNTAw\nWhcNMTgwNDA3MTQyNTAwWjBgMQswCQYDVQQGEwJERTERMA8GA1UEBxMIRXNjaGJv\ncm4xGzAZBgNVBAoTEkRldXRzY2hlIEJvZXJzZSBBRzENMAsGA1UECxMEUmlzazES\nMBAGA1UEAxMJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC\nAQEAwgkzsx+TDyCaIWTi3Ho4BgKlpKxL+ehndELr6riPooe9kuBwRimEPK7GCInw\nQBAu6FyDtR9HamOOjRUUEk0UcfBZNo0cMbeduGTcfG8XdICrfVd1qe6u/6EqgTip\nHsJ+gRePBezF06kU6wQagZ7qAshsKwF5Fbmz43iBog4wVcebg128aPnmE9eQufgx\n82VUr2PcD22tnYf3XT/2s3DuQCPZtxBMjpKYA1nGX0Cx32eP9WQP/vA0bYEOAW4v\nESd10X8P7cWcnKLe7r3s45US2zGH33ZXaPFxM1JRNqMyyfSzScT8nnxb+UuFPIkZ\nOpBV6jO2kIzx6/bLH8aydghbAwIDAQABo38wfTAOBgNVHQ8BAf8EBAMCBaAwHQYD\nVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHQYDVR0O\nBBYEFG6EKey01pP2cXCW0I7n7R66bhHGMB8GA1UdIwQYMBaAFC0Ucxnya/ds+SY/\nSRZhHLhgMazGMA0GCSqGSIb3DQEBCwUAA4IBAQBdLWPH2mqEtVAVCx5qf88joC5O\nkKgI+v/5c36JGmaOGH2LeF3LfXBZMEhopXQPUra/+AQVabKJSiH0UpsBXeiwBKNY\nuUNrMIqknvgMBm075RhcUvvwVN2K4tbGfG+7b4c7eFP9jofWxzbb/jBzO1rjJBzu\nAsyxCQxx9pFVYVa9af4GQ2SLlHdP/cyQN+Dn/rYgCJfH9EBK3p1uDZ3apDI6cVXt\nlyukl+hICVApqrxXor2wNMMK/0PyMSZMKngOgTDFFpeWxHeSbWxmiIPmbE8q0AT8\n2ttqvZK4Dp2YFxQR44V7UKcJyTvfgG+78bsr/E6FOzXBlrMP9prd48ylEwZk\n-----END CERTIFICATE-----";

    public static JsonObject getMongoClientConfig() {
        final String DB_NAME = "DAVe";
        return new JsonObject()
                .put("db_name", DB_NAME)
                .put("connection_string", String.format("mongodb://localhost:%s/?waitqueuemultiple=%d", MONGO_PORT, 20000));
    }

}
