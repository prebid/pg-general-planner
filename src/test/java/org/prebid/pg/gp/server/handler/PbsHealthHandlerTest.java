package org.prebid.pg.gp.server.handler;

import com.codahale.metrics.MetricRegistry;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.prebid.pg.gp.server.auth.BasicAuthUser;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.Shutdown;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class PbsHealthHandlerTest extends HandlerTestBase {

    @Mock
    private Shutdown shutdownMock;

    @Mock
    private MultiMap paramsMock;

    @Mock
    private RoutingContext routingContextMock;

    @Mock
    private HttpServerRequest httpRequestMock;

    @Mock
    private HttpServerResponse httpResponseMock;

    @Mock
    private CircuitBreakerSecuredPlannerDataAccessClient dataAccessClientMock;

    private PbsHealthHandler pbsHealthHandler;

    private ClassLoader classLoader = getClass().getClassLoader();

    private AlertProxyHttpClient alertHttpClientMock;

    private Metrics metrics = new Metrics(new MetricRegistry());

    @BeforeEach
    void setUp() {
        alertHttpClientMock = mock(AlertProxyHttpClient.class);
        int pbsMaxIdlePeriodInSeconds = 300;
        pbsHealthHandler = new PbsHealthHandler(
                dataAccessClientMock,
                pbsMaxIdlePeriodInSeconds,
                "error message",
                metrics,
                "admin",
                true,
                alertHttpClientMock,
                shutdownMock);

        given(routingContextMock.response()).willReturn(httpResponseMock);
    }

    @Test
    void shouldHandleProperly() {
        given(routingContextMock.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("admin", "admin", "admin"), "admin", "admin"));
        given(routingContextMock.request()).willReturn(httpRequestMock);
        given(httpRequestMock.params()).willReturn(paramsMock);
        given(paramsMock.get("vendor")).willReturn("vendor");
        given(paramsMock.get("region")).willReturn("region");
        given((paramsMock.get("instanceId"))).willReturn("instanceId");
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);
        List<Map<String, Object>> registrations = new ArrayList<>();
        given(dataAccessClientMock.findRegistrations(any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(registrations));

        pbsHealthHandler.handle(routingContextMock);

        verify(httpResponseMock).setStatusCode(HttpResponseStatus.OK.code());
        verify(httpResponseMock).end("[]");
    }

    @Test
    void shouldResponseWith403ForMissingCredential() {
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);
        pbsHealthHandler.handle(routingContextMock);
        verify(httpResponseMock).setStatusCode(HttpResponseStatus.FORBIDDEN.code());
        verify(httpResponseMock).end();
    }

    @Test
    void shouldResponseWith403ForWrongCredential() {
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);
        given(routingContextMock.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("foo", "un", "pwd"), "un", "foo"));
        pbsHealthHandler.handle(routingContextMock);
        verify(httpResponseMock).setStatusCode(HttpResponseStatus.FORBIDDEN.code());
        verify(httpResponseMock).end();
    }

    @Test
    void shouldNotProceedWhenShutdownStarted() {
        given(shutdownMock.getInitiating()).willReturn(true);
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);

        pbsHealthHandler.handle(routingContextMock);
        verify(httpResponseMock).setStatusCode(HttpResponseStatus.BAD_GATEWAY.code());
        verify(httpResponseMock).end(anyString());
    }
}
