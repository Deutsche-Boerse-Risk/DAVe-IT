package com.deutscheboerse.risk.dave.utils;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.HealthCheckIT;
import com.deutscheboerse.risk.dave.model.AbstractModel;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DataHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DataHelper.class);

    public static final String ACCOUNT_MARGIN_FOLDER = "accountMargin";
    public static final String LIQUI_GROUP_MARGIN_FOLDER = "liquiGroupMargin";
    public static final String LIQUI_GROUP_SPLIT_MARGIN_FOLDER = "liquiGroupSplitMargin";
    public static final String POOL_MARGIN_FOLDER = "poolMargin";
    public static final String POSITION_REPORT_FOLDER = "positionReport";
    public static final String RISK_LIMIT_UTILIZATION_FOLDER = "riskLimitUtilization";

    private static Optional<JsonArray> getJsonArrayFromTTSaveFile(String folderName, int ttsaveNo) {
        String jsonPath = String.format("%s/snapshot-%03d.json", HealthCheckIT.class.getResource(folderName).getPath(), ttsaveNo);
        try {
            byte[] jsonArrayBytes = Files.readAllBytes(Paths.get(jsonPath));
            JsonArray jsonArray = new JsonArray(new String(jsonArrayBytes, Charset.defaultCharset()));
            return Optional.of(jsonArray);
        } catch (IOException e) {
            LOG.error("Unable to read data from {}", jsonPath, e);
            return Optional.empty();
        }
    }

    public static void readTTSaveFile(String folderName, int ttsaveNo, Consumer<JsonObject> consumer) {
        getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .stream()
                .forEach(json -> consumer.accept((JsonObject) json));
    }

    public static Collection<JsonObject> readTTSaveFile(String folderName, int ttsaveNo) {
        return getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .stream()
                .map(json -> (JsonObject) json)
                .collect(Collectors.toList());
    }

    public static Optional<JsonObject> getLastJsonFromFile(String folderName, int ttsaveNo) {
        return getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .stream()
                .map(json -> (JsonObject) json)
                .reduce((a, b) -> b);
    }

    public static <T extends AbstractModel> T getLastModelFromFile(Class<T> clazz, int ttsaveNo) {
        String folderName = clazz.getSimpleName().substring(0, 1).toLowerCase() +
                clazz.getSimpleName().substring(1).replace("Model", "");
        JsonObject json = getLastJsonFromFile(folderName, ttsaveNo).orElse(new JsonObject());
        try {
            T model = clazz.newInstance();
            model.mergeIn(json);
            return model;
        } catch (IllegalAccessException|InstantiationException e) {
            throw new AssertionError();
        }
    }

    public static int getJsonObjectCount(String folderName, int ttsaveNo) {
        return getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .size();
    }

    public static JsonObject getQueryParams(AbstractModel model) {
        JsonObject queryParams = new JsonObject();
        model.getKeys().forEach(key -> queryParams.put(key, model.getValue(key)));
        return queryParams;
    }

    public static PrismaReports.AccountMargin createAccountMarginGPBFromJson(JsonObject json) {
        PrismaReports.AccountMargin result = PrismaReports.AccountMargin.newBuilder()
                .setKey(PrismaReports.AccountMarginKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setMember(json.getString("member"))
                        .setAccount(json.getString("account"))
                        .setMarginCurrency(json.getString("marginCurrency")))
                .setClearingCurrency(json.getString("clearingCurrency"))
                .setPool(json.getString("pool"))
                .setMarginReqInMarginCurr(json.getDouble("marginReqInMarginCurr"))
                .setMarginReqInClrCurr(json.getDouble("marginReqInClrCurr"))
                .setUnadjustedMarginRequirement(json.getDouble("unadjustedMarginRequirement"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build();
        return result;
    }

    public static PrismaReports.LiquiGroupMargin createLiquiGroupMarginGPBFromJson(JsonObject json) {
        PrismaReports.LiquiGroupMargin result = PrismaReports.LiquiGroupMargin.newBuilder()
                .setKey(PrismaReports.LiquiGroupMarginKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setMember(json.getString("member"))
                        .setAccount(json.getString("account"))
                        .setMarginClass(json.getString("marginClass"))
                        .setMarginCurrency(json.getString("marginCurrency")))
                .setMarginGroup(json.getString("marginGroup"))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setCurrentLiquidatingMargin(json.getDouble("currentLiquidatingMargin"))
                .setFuturesSpreadMargin(json.getDouble("futuresSpreadMargin"))
                .setAdditionalMargin(json.getDouble("additionalMargin"))
                .setUnadjustedMarginRequirement(json.getDouble("unadjustedMarginRequirement"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build();
        return result;
    }

    public static PrismaReports.LiquiGroupSplitMargin createLiquiGroupSplitMarginGPBFromJson(JsonObject json) {
        PrismaReports.LiquiGroupSplitMargin result = PrismaReports.LiquiGroupSplitMargin.newBuilder()
                .setKey(PrismaReports.LiquiGroupSplitMarginKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setMember(json.getString("member"))
                        .setAccount(json.getString("account"))
                        .setLiquidationGroup(json.getString("liquidationGroup"))
                        .setLiquidationGroupSplit(json.getString("liquidationGroupSplit"))
                        .setMarginCurrency(json.getString("marginCurrency")))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setMarketRisk(json.getDouble("marketRisk"))
                .setLiquRisk(json.getDouble("liquRisk"))
                .setLongOptionCredit(json.getDouble("longOptionCredit"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build();
        return result;
    }

    public static PrismaReports.PoolMargin createPoolMarginGPBFromJson(JsonObject json) {
        PrismaReports.PoolMargin result = PrismaReports.PoolMargin.newBuilder()
                .setKey(PrismaReports.PoolMarginKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setPool(json.getString("pool"))
                        .setMarginCurrency(json.getString("marginCurrency")))
                .setClrRptCurrency(json.getString("clrRptCurrency"))
                .setRequiredMargin(json.getDouble("requiredMargin"))
                .setCashCollateralAmount(json.getDouble("cashCollateralAmount"))
                .setAdjustedSecurities(json.getDouble("adjustedSecurities"))
                .setAdjustedGuarantee(json.getDouble("adjustedGuarantee"))
                .setOverUnderInMarginCurr(json.getDouble("overUnderInMarginCurr"))
                .setOverUnderInClrRptCurr(json.getDouble("overUnderInClrRptCurr"))
                .setVariPremInMarginCurr(json.getDouble("variPremInMarginCurr"))
                .setAdjustedExchangeRate(json.getDouble("adjustedExchangeRate"))
                .setPoolOwner(json.getString("poolOwner"))
                .build();
        return result;
    }

    public static PrismaReports.PositionReport createPositionReportGPBFromJson(JsonObject json) {
        PrismaReports.PositionReport result = PrismaReports.PositionReport.newBuilder()
                .setKey(PrismaReports.PositionReportKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setMember(json.getString("member"))
                        .setAccount(json.getString("account"))
                        .setLiquidationGroup(json.getString("liquidationGroup"))
                        .setLiquidationGroupSplit(json.getString("liquidationGroupSplit"))
                        .setProduct(json.getString("product"))
                        .setCallPut(json.getString("callPut"))
                        .setContractYear(json.getInteger("contractYear"))
                        .setContractMonth(json.getInteger("contractMonth"))
                        .setExpiryDay(json.getInteger("expiryDay"))
                        .setExercisePrice(json.getDouble("exercisePrice"))
                        .setVersion(json.getString("version"))
                        .setFlexContractSymbol(json.getString("flexContractSymbol"))
                )
                .setNetQuantityLs(json.getDouble("netQuantityLs"))
                .setNetQuantityEa(json.getDouble("netQuantityEa"))
                .setClearingCurrency(json.getString("clearingCurrency"))
                .setMVar(json.getDouble("mVar"))
                .setCompVar(json.getDouble("compVar"))
                .setCompCorrelationBreak(json.getDouble("compCorrelationBreak"))
                .setCompCompressionError(json.getDouble("compCompressionError"))
                .setCompLiquidityAddOn(json.getDouble("compLiquidityAddOn"))
                .setCompLongOptionCredit(json.getDouble("compLongOptionCredit"))
                .setProductCurrency(json.getString("productCurrency"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setNormalizedDelta(json.getDouble("normalizedDelta"))
                .setNormalizedGamma(json.getDouble("normalizedGamma"))
                .setNormalizedVega(json.getDouble("normalizedVega"))
                .setNormalizedRho(json.getDouble("normalizedRho"))
                .setNormalizedTheta(json.getDouble("normalizedTheta"))
                .setUnderlying(json.getString("underlying"))
                .build();
        return result;
    }

    public static PrismaReports.RiskLimitUtilization createRiskLimitUtilizationGPBFromJson(JsonObject json) {
        PrismaReports.RiskLimitUtilization.Builder builder = PrismaReports.RiskLimitUtilization.newBuilder()
                .setKey(PrismaReports.RiskLimitUtilizationKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setMember(json.getString("member"))
                        .setMaintainer(json.getString("maintainer"))
                        .setLimitType(json.getString("limitType")))
                .setUtilization(json.getDouble("utilization"));
        if (json.getDouble("warningLevel") != null) {
            builder.setWarningLevel(json.getDouble("warningLevel"));
        }
        if (json.getDouble("throttleLevel") != null) {
            builder.setThrottleLevel(json.getDouble("throttleLevel"));
        }
        if (json.getDouble("rejectLevel") != null) {
            builder.setRejectLevel(json.getDouble("rejectLevel"));
        }
        return builder.build();
    }

    public static PrismaReports.PrismaHeader createPrismaHeaderFromJson(JsonObject json) {
        PrismaReports.PrismaHeader result = PrismaReports.PrismaHeader.newBuilder()
                .setId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .build();
        return result;
    }
}
