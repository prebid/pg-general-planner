package org.prebid.pg.gp.server.handler;

import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class TrxIdHandlerTest {
    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerRequest httpRequest;

    @Mock
    private HttpServerResponse httpResponse;

    @Mock
    private MultiMap multiMap;

    @Test
    public void shouldHandlerAddTrxIdHeader() {
        final String trxId = "abe";
        
        given(routingContext.request()).willReturn(httpRequest);
        given(httpRequest.headers()).willReturn(multiMap);
        given(multiMap.get(TrxIdHandler.TRX_ID)).willReturn(trxId);
        given(routingContext.response()).willReturn(httpResponse);
        CaseInsensitiveHeaders responseHeaders = new CaseInsensitiveHeaders();
        given(httpResponse.headers()).willReturn(responseHeaders);

        new TrxIdHandler().handle(routingContext);

        verify(routingContext).request();
        verify(httpRequest).headers();
        verify(multiMap).get(TrxIdHandler.TRX_ID);
        verify(routingContext).response();
        verify(httpResponse).headers();
        assertThat(responseHeaders.get(TrxIdHandler.TRX_ID), equalTo(trxId));
    }

    @Test
    public void shouldHandlerNotAddTrxIdHeader() {
        given(routingContext.request()).willReturn(httpRequest);
        given(httpRequest.headers()).willReturn(multiMap);
        given(multiMap.get(TrxIdHandler.TRX_ID)).willReturn(null);

        new TrxIdHandler().handle(routingContext);

        verify(routingContext).request();
        verify(httpRequest).headers();
        verify(multiMap).get(TrxIdHandler.TRX_ID);
        verify(routingContext, never()).response();
    }
}
