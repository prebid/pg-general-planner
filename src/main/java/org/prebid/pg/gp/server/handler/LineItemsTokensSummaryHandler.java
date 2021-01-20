package org.prebid.pg.gp.server.handler;

import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.prebid.pg.gp.server.exception.InvalidRequestException;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.model.LineItemIdentity;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.spring.config.WebConfiguration.CsvMapperFactory;
import org.prebid.pg.gp.server.util.Constants;
import org.springframework.util.StringUtils;

import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A handler for retrieval of line item summary report request.
 */
public class LineItemsTokensSummaryHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(LineItemsTokensSummaryHandler.class);

    private static final Set<String> TOKEN_SUMMARY_METRICS = Collections.unmodifiableSet(
            new LinkedHashSet<>(Arrays.asList(
                    "id",
                    "summaryWindowStartTimestamp",
                    "summaryWindowEndTimestamp",
                    "lineItemId",
                    "tokens",
                    "bidderCode",
                    "extLineItemId")));

    private final CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient;

    private final CsvMapperFactory csvMapperFactory;

    private final String resourceRole;

    private final boolean securityEnabled;

    private final Metrics metrics;

    private final AlertProxyHttpClient alertHttpClient;

    private final Shutdown shutdown;

    public LineItemsTokensSummaryHandler(
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            CsvMapperFactory csvMapperFactory,
            String resourceRole,
            boolean securityEnabled,
            Metrics metrics,
            AlertProxyHttpClient alertHttpClient,
            Shutdown shutdown) {
        this.dataAccessClient = Objects.requireNonNull(plannerDataAccessClient);
        this.csvMapperFactory = Objects.requireNonNull(csvMapperFactory);
        this.resourceRole = Objects.requireNonNull(resourceRole);
        this.securityEnabled = securityEnabled;
        this.metrics = Objects.requireNonNull(metrics);
        this.alertHttpClient = Objects.requireNonNull(alertHttpClient);
        this.shutdown = Objects.requireNonNull(shutdown);
        logger.info("LineItemsTokensSummaryHandler protected by role {0}", resourceRole);
    }

    /**
     * Handles retrieval of line item summary report request.
     *
     * @param routingContext context of request and response
     */
    @Override
    public void handle(RoutingContext routingContext) {
        if (shutdown.getInitiating() == Boolean.TRUE) {
            routingContext.response()
                    .setStatusCode(HttpResponseStatus.BAD_GATEWAY.code())
                    .end("Server shutdown has been initiated");
            return;
        }

        if (securityEnabled) {
            if (routingContext.user() == null) {
                routingContext.response()
                        .setStatusCode(HttpResponseStatus.FORBIDDEN.code())
                        .end();
                return;
            }
            routingContext.user().isAuthorized(resourceRole, rs -> {
                if (rs.succeeded() && rs.result().booleanValue()) {
                    processRequest(routingContext);
                } else {
                    HttpServerResponse response = routingContext.response();
                    response.setStatusCode(HttpResponseStatus.FORBIDDEN.code()).end();
                }
            });
        } else {
            processRequest(routingContext);
        }
    }

    @SuppressWarnings({"squid:S1854", "squid:S1481"})
    private void processRequest(RoutingContext routingContext) {
        final AtomicReference<Request> reqReference = new AtomicReference<>();
        final long start = System.currentTimeMillis();
        metrics.incCounter(metricName("requests-served"));
        parseRequest(routingContext)
                .compose(req -> {
                    reqReference.set(req);
                    return getLineItemTokensSummary(req);
                })
                .setHandler(ar -> finalHandler(ar, routingContext, reqReference.get(), start));
    }

    private Future<List<LineItemsTokensSummary>> getLineItemTokensSummary(Request req) {
        Instant now = Instant.now();
        Instant currentHour = now.truncatedTo(ChronoUnit.HOURS);
        Instant tokenSummaryEndTime = req.getEndTimestamp().plusSeconds(1);
        if (!req.endTimestamp.isAfter(currentHour)) {
            return dataAccessClient.getLineItemsTokensSummary(
                    req.getStartTimestamp(), tokenSummaryEndTime, req.getUniqueLineItemIds());
        } else if (!req.getStartTimestamp().isBefore(currentHour)) {
            return dataAccessClient.getHourlyLineItemTokens(
                    req.getStartTimestamp(), req.getEndTimestamp(), req.getLineItems());
        }
        return dataAccessClient.getLineItemsTokensSummary(
                req.getStartTimestamp(), tokenSummaryEndTime, req.getUniqueLineItemIds())
                .compose(summaries -> {
                    Instant start = summaries.isEmpty()
                            ? req.getStartTimestamp()
                            : summaries.get(summaries.size() - 1).getSummaryWindowEndTimestamp();
                    return dataAccessClient.getHourlyLineItemTokens(
                            start, req.getEndTimestamp(), req.getLineItems())
                            .map(latestSummaries -> {
                                summaries.addAll(latestSummaries);
                                return summaries;
                            });
                });
    }

    private void finalHandler(AsyncResult<List<LineItemsTokensSummary>> asyncResult,
            RoutingContext routingContext, Request request, long startTime) {
        HttpServerResponse response = routingContext.response();
        response.putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);

        if (asyncResult.succeeded()) {
            try {
                final CsvMapper csvMapper = csvMapperFactory.getCsvMapper();
                setCsvFilters(csvMapper, request.getMetrics());
                String csv = csvMapper.writerFor(List.class)
                        .with(csvSchema(request.getMetrics()))
                        .writeValueAsString(asyncResult.result());
                response.setStatusCode(HttpResponseStatus.OK.code()).end(csv);
            } catch (Exception ex) {
                handleErrorResponse(response, ex);
            }
        } else {
            handleErrorResponse(response, asyncResult.cause());
        }
        metrics.updateTimer(metricName("processing-time"), System.currentTimeMillis() - startTime);
    }

    private void handleErrorResponse(HttpServerResponse response, Throwable exception) {
        metrics.incCounter(metricName("exc"));
        if (exception instanceof InvalidRequestException) {
            response.setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
            return;
        }
        alertHttpClient.raiseEvent(
                Constants.GP_PLANNER_LINE_ITEM_TOKENS_SUMMARY_HANDLER_ERROR,
                AlertPriority.MEDIUM,
                String.format("Exception in LineItemTokensSummaryHandler::%s", exception.getMessage()));
        response.setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end();
    }

    private void setCsvFilters(CsvMapper csvMapper, Set<String> requestMetrics) {
        SimpleBeanPropertyFilter filter = new SimpleBeanPropertyFilter.FilterExceptFilter(requestMetrics);
        csvMapper.setFilterProvider(new SimpleFilterProvider().addFilter(CsvAnnotationIntrospector.FILTER_ID, filter));
        csvMapper.setAnnotationIntrospector(new CsvAnnotationIntrospector());
    }

    static class CsvAnnotationIntrospector extends JacksonAnnotationIntrospector {
        static final String FILTER_ID = "CSV_FILTER";

        @Override
        public Object findFilterId(Annotated annotated) {
            return FILTER_ID;
        }

    }

    private Future<Request> parseRequest(RoutingContext routingContext) {
        final MultiMap params = routingContext.request().params();
        final String startTimeStr = params.get("startTime");
        final String lineItems = params.get("lineItemIds");

        logger.info("Request query params::{0}", params.entries());

        Instant startTimestamp;
        if (!StringUtils.isEmpty(startTimeStr)) {
            try {
                startTimestamp = Instant.parse(startTimeStr);
            } catch (DateTimeParseException ex) {
                return Future.failedFuture(
                        new InvalidRequestException(GPConstants.BAD_FORMAT, "Bad startTime format"));
            }
        } else {
            startTimestamp = Instant.now().minus(60, ChronoUnit.SECONDS);
        }
        startTimestamp = startTimestamp.truncatedTo(ChronoUnit.HOURS);

        Instant endTimestamp = Instant.now().minus(0, ChronoUnit.SECONDS);
        final Instant maxEndTimestamp = startTimestamp.plus(345600, ChronoUnit.SECONDS);
        if (maxEndTimestamp.isBefore(endTimestamp)) {
            endTimestamp = maxEndTimestamp;
        }

        logger.info(
                "Request::Mapped params::startTimestamp={0}, endTimestamp={1}, lineItems={2}",
                startTimestamp, endTimestamp, lineItems
        );

        List<LineItemIdentity> lineItemIds = new ArrayList<>();
        List<String> uniqueLineItemIds = new ArrayList<>();
        if (!StringUtils.isEmpty(lineItems)) {
            String[] lis = lineItems.split(",");
            for (String li : lis) {
                uniqueLineItemIds.add(li);
                String[] parts = li.split("-");
                if (parts.length != 2) {
                    logger.info("Bad lineItemId format: {0}", li);
                    return Future.failedFuture(
                            new InvalidRequestException(GPConstants.BAD_FORMAT, "Bad lineItemId format"));
                } else {
                    lineItemIds.add(LineItemIdentity.builder()
                            .bidderCode(parts[0])
                            .lineItemId(parts[1])
                            .build());
                }
            }
        }
        Request req = Request.builder()
                .startTimestamp(startTimestamp)
                .endTimestamp(endTimestamp)
                .metrics(new LinkedHashSet<>(TOKEN_SUMMARY_METRICS))
                .uniqueLineItemIds(uniqueLineItemIds)
                .lineItems(lineItemIds)
                .build();
        logger.debug("LineItemsTokensSummary request object::{0}", req);
        return Future.succeededFuture(req);
    }

    public static CsvSchema csvSchema(Iterable<String> requestMetrics) {
        CsvSchema.Builder schemaBuilder = new CsvSchema.Builder().setUseHeader(true);
        for (String column : requestMetrics) {
            if ("tokens".equals(column)) {
                schemaBuilder.addColumn(column, CsvSchema.ColumnType.NUMBER);
            } else {
                schemaBuilder.addColumn(column);
            }
        }
        return schemaBuilder.build();
    }

    private String metricName(String tag) {
        return String.format("line-items-tokens-summary-request.%s", tag);
    }

    @Builder
    @Getter
    @ToString
    static class Request {

        private final Instant startTimestamp;

        private final Instant endTimestamp;

        private final Set<String> metrics;

        private final List<String> uniqueLineItemIds;

        private final List<LineItemIdentity> lineItems;
    }

}
