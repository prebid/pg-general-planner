package org.prebid.pg.gp.server.auth;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AbstractUser;
import io.vertx.ext.auth.AuthProvider;

import java.util.Objects;

/**
 * An extension of {@code AbstractUser} to represent a user of General Planner.
 */
public class BasicAuthUser extends AbstractUser {
    private final String username;
    private final JsonObject principal;
    private BasicAuthProvider basicAuthProvider;

    /**
     * Constructor.
     *
     * @param basicAuthProvider a {@link BasicAuthProvider} instance
     * @param username the username
     * @param roles a comma separate {@code String} of roles
     */
    public BasicAuthUser(BasicAuthProvider basicAuthProvider, String username, String roles) {
        this.basicAuthProvider = Objects.requireNonNull(basicAuthProvider);
        this.username = Objects.requireNonNull(username);
        principal = new JsonObject().put("username", username).put("roles", roles);
    }

    @Override
    protected void doIsPermitted(String resourceRole, Handler<AsyncResult<Boolean>> resultHandler) {
        basicAuthProvider.authorize(username, resourceRole, resultHandler);
    }

    @Override
    public JsonObject principal() {
        return principal;
    }

    @Override
    public void setAuthProvider(AuthProvider authProvider) {
        if (authProvider instanceof BasicAuthProvider) {
            this.basicAuthProvider = (BasicAuthProvider) authProvider;
        } else {
            throw new IllegalArgumentException("Not a BasicAuthProvider");
        }
    }
}
