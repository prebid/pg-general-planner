package org.prebid.pg.gp.server.auth;

import io.vertx.ext.auth.AuthProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class BasicAuthUserTest {

    @Test
    void shouldSetAuthProvider() {
        BasicAuthProvider authProviderMock = mock(BasicAuthProvider.class);
        BasicAuthUser authUser = new BasicAuthUser(authProviderMock, "username", "role");
        authUser.setAuthProvider(mock(BasicAuthProvider.class));
        assertThrows(IllegalArgumentException.class, () -> authUser.setAuthProvider(mock(AuthProvider.class)));
    }
}
