package org.prebid.pg.gp.server.handler;

import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Resources;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.prebid.pg.gp.server.auth.BasicAuthProvider;
import org.prebid.pg.gp.server.auth.BasicAuthUser;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminEvent;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.Registration;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.model.TracerFilters;
import org.prebid.pg.gp.server.spring.config.app.ServerAuthDataConfiguration;

import java.io.File;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.prebid.pg.gp.server.handler.PbsRegistrationHandler.REG_REQUEST_KEY;

@ExtendWith(MockitoExtension.class)
class PbsRegistrationHandlerTest {

    @Mock
    private Shutdown shutdown;

    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerResponse httpResponse;

    @Mock
    private CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient;

    private PbsRegistrationHandler pbsRegistrationHandler;

    private AdminTracer tracer;

    private String vendor = "vendor1";

    @BeforeEach
    void setUp() {
        tracer = new AdminTracer();
        tracer.setEnabled(true);
        tracer.setExpiresAt(Instant.now().plusSeconds(86400));
        TracerFilters filters = new TracerFilters();
        filters.setRegion("us-east");
        filters.setVendor("vendor1");
        tracer.setFilters(filters);

        pbsRegistrationHandler = new PbsRegistrationHandler(
                dataAccessClient,
                "Service is temporarily unavailable, please try again later",
                "pbs", true, new Metrics(new MetricRegistry()), false, tracer, shutdown
        );

        given(routingContext.response()).willReturn(httpResponse);
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);
    }

    private BasicAuthProvider getBasicAuthProvider(String roles) {
        ServerAuthDataConfiguration serverAuthDataConfiguration = new ServerAuthDataConfiguration();
        serverAuthDataConfiguration.setAuthenticationEnabled(true);

        List<ServerAuthDataConfiguration.Principal> principals = new ArrayList<>();
        ServerAuthDataConfiguration.Principal principal = new ServerAuthDataConfiguration.Principal();
        principal.setUsername("user1");
        principal.setPassword("password1");
        principal.setRoles(roles);
        principals.add(principal);

        serverAuthDataConfiguration.setPrincipals(principals);
        return new BasicAuthProvider(serverAuthDataConfiguration);
    }

    @Test
    void shouldRespondWithHttpStatus403AndEmptyResponseBodyOnInvalidRole() throws Exception {
        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbsx"), "user1", "pbsx"));

        pbsRegistrationHandler.handle(routingContext);

        verify(httpResponse).end();
        verify(httpResponse).setStatusCode(HttpResponseStatus.FORBIDDEN.code());
    }

    @Test
    void shouldRespondWithHttpStatus200AndEmptyResponseBodyOnValidPayloadWhenTracerEnabled() throws Exception {
        shouldRespondWithHttpStatus200AndEmptyResponseBodyOnValidPayload();
    }

    @Test
    void shouldRespondWithHttpStatus200AndEmptyResponseBodyOnValidPayloadWhenTracerDisabled() throws Exception {
        tracer.setEnabled(false);
        shouldRespondWithHttpStatus200AndEmptyResponseBodyOnValidPayload();
    }

    private void shouldRespondWithHttpStatus200AndEmptyResponseBodyOnValidPayload() throws Exception {
        String baseDir = "pbs-register/sunny-day";
        String regFileName = "register.json";
        URL url = Resources.getResource(String.format("%s/input/%s", baseDir, regFileName));
        String body = FileUtils.readFileToString(new File(url.toURI()), "UTF-8");
        given(routingContext.getBody()).willReturn(Buffer.buffer(body));

        given(dataAccessClient.updateRegistration(any()))
                .willReturn(Future.succeededFuture(new UpdateResult()));
        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        pbsRegistrationHandler.handle(routingContext);

        verify(httpResponse).end();
        verify(httpResponse).setStatusCode(HttpResponseStatus.OK.code());
    }

    @Test
    void shouldIncludeAdminDirectiveIfAny() throws Exception {
        String baseDir = "pbs-register/sunny-day";
        String regFileName = "register.json";
        URL url = Resources.getResource(String.format("%s/input/%s", baseDir, regFileName));
        String body = FileUtils.readFileToString(new File(url.toURI()), "UTF-8");
        given(routingContext.getBody()).willReturn(Buffer.buffer(body));
        Registration registration = Registration.builder()
                .region("east")
                .vendor(vendor)
                .instanceId("foo")
                .build();
        AdminTracer tracer = new AdminTracer();
        tracer.setFilters(new TracerFilters());
        AdminEvent adminEvent = AdminEvent.builder()
                .app("pbs")
                .vendor(vendor)
                .region("east")
                .instanceId("foo")
                .expiryAt(Instant.now().plusSeconds(300))
                .directive(AdminEvent.Directive.builder()
                        .tracer(tracer)
                        .build())
                .build();
        given(routingContext.get(REG_REQUEST_KEY)).willReturn(registration);
        given(dataAccessClient.updateRegistration(any()))
                .willReturn(Future.succeededFuture(new UpdateResult()));
        given(dataAccessClient.findEarliestActiveAdminEvent(any(), any(), any()))
                .willReturn(Future.succeededFuture(adminEvent));

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        pbsRegistrationHandler.handle(routingContext);

        verify(httpResponse).end(anyString());
        verify(httpResponse).setStatusCode(HttpResponseStatus.OK.code());
    }

    @Test
    void shouldRespondWithHttpStatus500OnBackendError() throws Exception {
        String baseDir = "pbs-register/sunny-day";
        String regFileName = "register.json";
        URL url = Resources.getResource(String.format("%s/input/%s", baseDir, regFileName));
        String body = FileUtils.readFileToString(new File(url.toURI()), "UTF-8");
        given(routingContext.getBody()).willReturn(Buffer.buffer(body));

        given(dataAccessClient.updateRegistration(any()))
                .willReturn(Future.failedFuture(new NullPointerException()));

        given(dataAccessClient.updateRegistration(any()))
                .willReturn(Future.failedFuture(new NullPointerException()));

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        pbsRegistrationHandler.handle(routingContext);

        final ArgumentCaptor<String> planRequestResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpResponse).end(planRequestResponseCaptor.capture());

        verify(httpResponse).setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());

        final String expected = "[\"Service is temporarily unavailable, please try again later\"]";
        assertThat(planRequestResponseCaptor.getValue(), equalTo(expected));
    }

    @Test
    void shouldRespondWithHttpStatus400OnMissingRegionFieldInPayload() throws Exception {
        String baseDir = "pbs-register/cloudy";
        String regFileName = "register-1.json";
        URL url = Resources.getResource(String.format("%s/input/%s", baseDir, regFileName));
        String body = FileUtils.readFileToString(new File(url.toURI()), "UTF-8");
        given(routingContext.getBody()).willReturn(Buffer.buffer(body));

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        pbsRegistrationHandler.handle(routingContext);

        final ArgumentCaptor<String> planRequestResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpResponse).end(planRequestResponseCaptor.capture());

        verify(httpResponse).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());

        final String expected =
                "[\"Cannot decode incoming health report::Missing fields/improper values::[region, vendor]\"]";
        assertThat(planRequestResponseCaptor.getValue(), equalTo(expected));
    }

    @Test
    void shouldRespondWithHttpStatus400OnEmptyRegionFieldInPayload() throws Exception {
        String baseDir = "pbs-register/cloudy";
        String regFileName = "register-2.json";
        URL url = Resources.getResource(String.format("%s/input/%s", baseDir, regFileName));
        String body = FileUtils.readFileToString(new File(url.toURI()), "UTF-8");
        given(routingContext.getBody()).willReturn(Buffer.buffer(body));

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        pbsRegistrationHandler.handle(routingContext);

        final ArgumentCaptor<String> planRequestResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpResponse).end(planRequestResponseCaptor.capture());

        verify(httpResponse).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());

        final String expected =
                "[\"Cannot decode incoming health report::Missing fields/improper values::[region, vendor]\"]";
        assertThat(planRequestResponseCaptor.getValue(), equalTo(expected));
    }


    @Test
    void shouldRespondWithHttpStatus400OnUnrecognizedFieldInPayload() throws Exception {
        String baseDir = "pbs-register/cloudy";
        String regFileName = "register-3.json";
        URL url = Resources.getResource(String.format("%s/input/%s", baseDir, regFileName));
        String body = FileUtils.readFileToString(new File(url.toURI()), "UTF-8");
        given(routingContext.getBody()).willReturn(Buffer.buffer(body));

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        pbsRegistrationHandler.handle(routingContext);

        final ArgumentCaptor<String> planRequestResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpResponse).end(planRequestResponseCaptor.capture());
        verify(httpResponse).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
        assertThat(planRequestResponseCaptor.getValue(), containsString("Unrecognized field"));
    }

    @Test
    void shouldRespondWithHttpStatus400OnNullPayload() throws Exception {
        given(routingContext.getBody()).willReturn(null);

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        pbsRegistrationHandler.handle(routingContext);

        final ArgumentCaptor<String> planRequestResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpResponse).end(planRequestResponseCaptor.capture());

        verify(httpResponse).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());

        final String expected = "[\"Incoming request has no body\"]";
        assertThat(planRequestResponseCaptor.getValue(), equalTo(expected));
    }

    @Test
    void shouldRespondWithHttpStatus400OnEmptyPayload() throws Exception {
        given(routingContext.getBody()).willReturn(Buffer.buffer("  "));

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        pbsRegistrationHandler.handle(routingContext);

        final ArgumentCaptor<String> planRequestResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpResponse).end(planRequestResponseCaptor.capture());

        verify(httpResponse).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());

        assertThat(planRequestResponseCaptor.getValue(), containsString("Failed to decode"));
    }
}