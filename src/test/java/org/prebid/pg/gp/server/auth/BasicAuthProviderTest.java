package org.prebid.pg.gp.server.auth;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.prebid.pg.gp.server.spring.config.app.ServerAuthDataConfiguration;
import org.prebid.pg.gp.server.spring.config.app.ServerAuthDataConfiguration.Principal;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class BasicAuthProviderTest {

    @Test
    public void shouldAuthorizeThrowExceptionIfAuthenticationDisabled() {
        BasicAuthProvider authProvider = createBasicAuthProvider(false);
        assertThrows(IllegalStateException.class,
                () -> authProvider.authorize("foo", "bar", rs -> {}));
    }

    @Test
    public void shouldAuthenticateThrowExceptionIfAuthenticationDisabled() {
        BasicAuthProvider authProvider = createBasicAuthProvider(false);
        assertThrows(IllegalStateException.class,
                () -> authProvider.authenticate(new JsonObject(), rs -> {}));
    }

    @Test
    void shouldAuthenticateFailedIfMissingUsername() {
        BasicAuthProvider authProvider = createBasicAuthProvider(true);
        JsonObject authInfo = new JsonObject();
        authProvider.authenticate(authInfo, rs -> {
           assertThat(rs.failed(), equalTo(true));
        });
    }

    @Test
    void shouldAuthenticateFailedIfMissingPassword() {
        BasicAuthProvider authProvider = createBasicAuthProvider(true);
        JsonObject authInfo = new JsonObject("{\"username\": \"un\"}");
        authProvider.authenticate(authInfo, rs -> {
            assertThat(rs.failed(), equalTo(true));
        });
    }

    @Test
    void shouldAuthenticate() {
        BasicAuthProvider authProvider = createBasicAuthProvider(true);
        JsonObject authInfo = new JsonObject("{\"username\": \"un\", \"password\":\"pwd\"}");
        authProvider.authenticate(authInfo, rs -> {
            assertThat(rs.succeeded(), equalTo(true));
        });
    }

    @Test
    void shouldAuthenticateFailedIfNotInRole() {
        BasicAuthProvider authProvider = createBasicAuthProvider(true);
        JsonObject authInfo = new JsonObject("{\"username\": \"foo\", \"password\":\"pwd\"}");
        authProvider.authenticate(authInfo, rs -> {
            assertThat(rs.failed(), equalTo(true));
        });
    }

    

    private BasicAuthProvider createBasicAuthProvider(boolean authenticationEnable) {
        ServerAuthDataConfiguration authConfiguration = new ServerAuthDataConfiguration();
        Principal user = new Principal();
        user.setPassword("pwd");
        user.setUsername("un");
        authConfiguration.setPrincipals(Arrays.asList(user));
        authConfiguration.setAuthenticationEnabled(authenticationEnable);
        BasicAuthProvider authProvider = new BasicAuthProvider(authConfiguration);
        return authProvider;
    }


}
