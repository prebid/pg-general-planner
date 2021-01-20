package org.prebid.pg.gp.server.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.prebid.pg.gp.server.auth.BasicAuthUser;
import org.prebid.pg.gp.server.model.Shutdown;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class CeaseShutdownHandlerTest extends HandlerTestBase {

    @Mock
    private RoutingContext routingContextMock;

    @Mock
    private HttpServerRequest httpRequest;

    @Mock
    private HttpServerResponse httpResponseMock;

    @Mock
    private Shutdown shutdownMock;

    private String resourceRole;

    private boolean securityEnabled = true;

    private CeaseShutdownHandler ceaseShutdownHandler;

    @BeforeEach
    void setUp() {
        this.resourceRole = "admin";
        this.ceaseShutdownHandler = new CeaseShutdownHandler(shutdownMock, resourceRole, securityEnabled);
        given(routingContextMock.response()).willReturn(httpResponseMock);
    }

    @Test
    void shouldResponseWith403ForMissingCredential() {
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);
        ceaseShutdownHandler.handle(routingContextMock);
        verify(httpResponseMock).setStatusCode(HttpResponseStatus.FORBIDDEN.code());
        verify(httpResponseMock).end();
    }

    @Test
    void shouldResponseWith403ForWrongCredential() {
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);
        given(routingContextMock.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("foo", "un", "pwd"), "un", "foo"));
        ceaseShutdownHandler.handle(routingContextMock);
        verify(httpResponseMock).setStatusCode(HttpResponseStatus.FORBIDDEN.code());
        verify(httpResponseMock).end();
    }

    @Test
    void shouldResponseOK() {
        given(routingContextMock.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("admin", "admin", "admin"), "admin", "admin"));
        given(httpResponseMock.setStatusCode(anyInt())).willReturn(httpResponseMock);
        ArgumentCaptor<Boolean> argument = ArgumentCaptor.forClass(Boolean.class);

        ceaseShutdownHandler.handle(routingContextMock);
        verify(httpResponseMock).setStatusCode(HttpResponseStatus.OK.code());
        verify(shutdownMock).setInitiating(argument.capture());
        assertThat(argument.getValue(), equalTo(false));
    }
}
