package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.json.JsonObject;

public class TestConfig {

    public static final String DAVE_API_IP = System.getProperty("api.ip");
    public static final String DAVE_STORE_MANAGER_IP = System.getProperty("store.manager.ip");
    public static final String DAVE_MARGIN_LOADER_IP = System.getProperty("margin.loader.ip");
    public static final String BROKER_IP = System.getProperty("cil.ip");
    public static final String MONGO_IP = System.getProperty("mongodb.ip");

    public static final int MONGO_PORT = 27017;
    public static final int BROKER_PORT = 5672;
    public static final int DAVE_API_HTTP_PORT = 8443;

    public static final int DAVE_API_HEALTHCHECK_PORT = 8080;
    public static final int DAVE_STOREMANAGER_HEALTHCHECK_PORT = 8080;
    public static final int DAVE_MARGINLOADER_HEALTHCHECK_PORT = 8080;

    public static final String DAVE_API_CERTIFICATE = "-----BEGIN CERTIFICATE-----\n" +
            "MIID4DCCAsigAwIBAgIUNfzsWakROzpI1zBnimJfLzV6g54wDQYJKoZIhvcNAQEL\n" +
            "BQAwTDELMAkGA1UEBhMCREUxETAPBgNVBAcTCEVzY2hib3JuMRswGQYDVQQKExJE\n" +
            "ZXV0c2NoZSBCb2Vyc2UgQUcxDTALBgNVBAsTBFJpc2swHhcNMTcwNjA3MDgzMjAw\n" +
            "WhcNMTgwNjA3MDgzMjAwWjBgMQswCQYDVQQGEwJERTERMA8GA1UEBxMIRXNjaGJv\n" +
            "cm4xGzAZBgNVBAoTEkRldXRzY2hlIEJvZXJzZSBBRzENMAsGA1UECxMEUmlzazES\n" +
            "MBAGA1UEAxMJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC\n" +
            "AQEA7nLhqksw2LSH28yBWaBItQpar9uDf+U6MqnPtiEE2AsGkcq0BrWRjqWxZqdR\n" +
            "2DkuqhJWfuw46/uOg7mGncxwiwuDquztkcq8rqLyULGKTjU2tJzZ68JDn3YyJhak\n" +
            "8OaGCF5L6y8Gj7R95T2CCPoZFKaXRfJdYcn6z1uYFJpUD7ntjrulqleYf8zhJLap\n" +
            "dUzl62qlOruqR7P8OirX/vbCC755doCZkfR/tHCR+Yu9RJa2teIqYXtsdnw4kw79\n" +
            "lRe0G/IQVZwlkNxUKmsul9qU/CjyWT5AOgm4A1QiLhk5FKuRlch4TtJ4EPIc2XmJ\n" +
            "IQmpIgpjlR28no9WuR24SW19/wIDAQABo4GlMIGiMA4GA1UdDwEB/wQEAwIFoDAd\n" +
            "BgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwDAYDVR0TAQH/BAIwADAdBgNV\n" +
            "HQ4EFgQU+J/pYkbTtyMohebE+1guEM9Zbv0wHwYDVR0jBBgwFoAUn9LsfNcAHaJI\n" +
            "+BNTF+0efLutuKkwIwYDVR0RBBwwGoIIZGF2ZS5hcGmCCGRhdmUtYXBphwR/AAAB\n" +
            "MA0GCSqGSIb3DQEBCwUAA4IBAQBJr70SKdGyt62SM5pIuE7kUZLhKBEFQCVvDRmQ\n" +
            "F7n67rWvoSQA1iZDqaWIahDg/hxa2dVkKnr3zoGLRGuVkgtmLfRo2foOHt+101IC\n" +
            "y84SO+w6Mge2bBf9MyJMXgwIZYTGOq3FVhh90liOHxKyPUbbwC5MC8RpOWTlYSFl\n" +
            "3tbN05wol+aOfv+/SWA2cp64CEp7OAESY+3+KQry1LzSbmziWI/8WIWmLh1Ipp4K\n" +
            "CIsUhicuKGVyCP/KvjOTPkiKoragoyJbJPCIDdMMwIFuq+f/rG98tph2SqMciPu0\n" +
            "iZjoLM+tN0YbvhAKHg1xfE9Xl08sVzOUuXBTXokZgcaUv6W0\n" +
            "-----END CERTIFICATE-----\n";

    public static JsonObject getMongoClientConfig() {
        final String DB_NAME = "DAVe";
        return new JsonObject()
                .put("db_name", DB_NAME)
                .put("connection_string", String.format("mongodb://%s:%s/?waitqueuemultiple=%d", MONGO_IP, MONGO_PORT, 20000));
    }

}
