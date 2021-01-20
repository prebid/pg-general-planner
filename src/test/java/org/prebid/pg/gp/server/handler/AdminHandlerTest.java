package org.prebid.pg.gp.server.handler;

import com.google.common.io.Resources;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
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
import org.prebid.pg.gp.server.model.AdminEvent;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.spring.config.app.ServerAuthDataConfiguration;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class AdminHandlerTest {

    private AdminTracer adminTracer = new AdminTracer();

    private int maxDurationInSeconds= 900;

    private int pbsMaxIdlePeriodInSeconds = 180;

    private String applicationListStr = "gp, pbs, delstats";

    private int batchSize = 10;

    @Mock
    private CircuitBreakerSecuredPlannerDataAccessClient dataAccessClientMock;

    @Mock
    private HttpServerResponse httpResponseMock;

    @Mock
    private Shutdown shutdown;

    private String resourceRole = "admin";

    private String vendor = "vendor1";

    @Mock
    private RoutingContext routingContextMock;

    @BeforeEach
    void setUp() {
    }

    @Test
    void shouldHandleRequestProperly() throws Exception {
        String body = loadRequest("admin-handler/sunny-request.json");
        PbsHost pbsHost = PbsHost.builder().region("us-east-1").vendor(vendor).hostInstanceId("foo").build();
        List<PbsHost> hosts = Arrays.asList(pbsHost);
        ArgumentCaptor<List<AdminEvent>> listCaptor = ArgumentCaptor.forClass(List.class);
        given(routingContextMock.getBody()).willReturn(Buffer.buffer(body));
        given(dataAccessClientMock.findActiveHosts(any())).willReturn(Future.succeededFuture(hosts));
        given(dataAccessClientMock.updateAdminEvents(listCaptor.capture(), anyInt()))
                .willReturn(Future.succeededFuture());

        given(routingContextMock.response()).willReturn(httpResponseMock);
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);

        adminHandler().handle(routingContextMock);

        verify(httpResponseMock).setStatusCode(HttpResponseStatus.OK.code());
        verify(httpResponseMock).end();
        List<AdminEvent> events = listCaptor.getValue();

        assertThat(events.size(), equalTo(1));
        AdminEvent adminEvent = events.get(0);
        assertThat(adminEvent.getApp(), equalTo("pbs"));
        assertThat(adminEvent.getVendor(), equalTo(vendor));
        assertThat(adminEvent.getRegion(), equalTo("us-east-1"));
        assertThat(adminEvent.getDirective().getTracer().getCmd(), equalTo("start"));
    }

    @Test
    void shouldSecurityCheck() {
        given(routingContextMock.user()).willReturn(null);
        given(routingContextMock.response()).willReturn(httpResponseMock);
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);
        adminHandler(true).handle(routingContextMock);
        verify(httpResponseMock).setStatusCode(HttpResponseStatus.FORBIDDEN.code());
        verify(httpResponseMock).end();
    }

    @Test
    void shouldUserAuthorize() {
        given(routingContextMock.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs"));
        given(routingContextMock.response()).willReturn(httpResponseMock);
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);
        adminHandler(true).handle(routingContextMock);
        verify(httpResponseMock).setStatusCode(HttpResponseStatus.FORBIDDEN.code());
        verify(httpResponseMock).end();
    }

    @Test
    void shouldHandleRequestForAdminTrackerProperly() throws Exception {
        String body = loadRequest("admin-handler/sunny-pg-request.json");
        PbsHost pbsHost = PbsHost.builder().region("us-east-1").vendor(vendor).hostInstanceId("foo").build();
        List<PbsHost> hosts = Arrays.asList(pbsHost);
        given(routingContextMock.getBody()).willReturn(Buffer.buffer(body));

        given(routingContextMock.response()).willReturn(httpResponseMock);
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);

        adminHandler().handle(routingContextMock);

        verify(httpResponseMock).setStatusCode(HttpResponseStatus.OK.code());
        verify(httpResponseMock).end();
    }

    @Test
    void shouldHandleBadRequest() throws Exception {
        String body = loadRequest("admin-handler/missing-app-request.json");
        given(routingContextMock.getBody()).willReturn(Buffer.buffer(body));

        given(routingContextMock.response()).willReturn(httpResponseMock);
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);

        adminHandler().handle(routingContextMock);

        verify(httpResponseMock).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void shouldHandleNotSupportedApps() throws Exception {
        this.applicationListStr = "foo, bar";
        String body = loadRequest("admin-handler/sunny-request.json");
        given(routingContextMock.getBody()).willReturn(Buffer.buffer(body));

        given(routingContextMock.response()).willReturn(httpResponseMock);
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);

        adminHandler().handle(routingContextMock);

        verify(httpResponseMock).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void shouldHandleNullBody() throws Exception {
        given(routingContextMock.getBody()).willReturn(null);

        given(routingContextMock.response()).willReturn(httpResponseMock);
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);

        adminHandler().handle(routingContextMock);

        verify(httpResponseMock).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void shouldHandleBadJsonRequest() throws Exception {
        String body = "{\"hello\": \"world\"}";
        given(routingContextMock.getBody()).willReturn(Buffer.buffer(body));

        given(routingContextMock.response()).willReturn(httpResponseMock);
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);

        adminHandler().handle(routingContextMock);

        verify(httpResponseMock).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
    }

    private AdminHandler adminHandler(boolean securityEnabled) {

        return new AdminHandler(
                adminTracer,
                maxDurationInSeconds,
                pbsMaxIdlePeriodInSeconds,
                applicationListStr,
                batchSize,
                dataAccessClientMock,
                resourceRole,
                securityEnabled,
                shutdown
        );
    }

    private AdminHandler adminHandler() {
        return adminHandler(false);
    }

    private String loadRequest(String path) throws Exception {
        URL url = Resources.getResource(path);
        return FileUtils.readFileToString(new File(url.toURI()), "UTF-8");
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
}
