package org.prebid.pg.gp.server.auth;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.spring.config.app.ServerAuthDataConfiguration;
import org.prebid.pg.gp.server.spring.config.app.ServerAuthDataConfiguration.Principal;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * An implementation of {@code AuthProvider} for user authentication and authorization.
 */
public class BasicAuthProvider implements AuthProvider {

    private static final Logger logger = LoggerFactory.getLogger(BasicAuthProvider.class);

    private final ServerAuthDataConfiguration serverAuthDataConfig;

    public BasicAuthProvider(ServerAuthDataConfiguration serverAuthDataConfig) {
        this.serverAuthDataConfig = Objects.requireNonNull(serverAuthDataConfig);
        logger.info("ServerAuthDataConfiguration={0}", serverAuthDataConfig);
    }

    @Override
    public void authenticate(JsonObject authInfo, Handler<AsyncResult<User>> resultHandler) {
        if (!serverAuthDataConfig.isAuthenticationEnabled()) {
            throw new IllegalStateException("BasicAuthentication is not enabled.");
        }

        String username = authInfo.getString("username");
        logger.debug("authInfo.username={0}", username);
        if (username == null) {
            resultHandler.handle(Future.failedFuture("authInfo must contain username"));
            return;
        }

        String password = authInfo.getString("password");
        if (password == null) {
            resultHandler.handle(Future.failedFuture("authInfo must contain password"));
            return;
        }

        for (Principal principal : serverAuthDataConfig.getPrincipals()) {
            logger.debug(principal);
            if (principal.getUsername().equals(username) && principal.getPassword().equals(password)) {
                resultHandler.handle(Future.succeededFuture(new BasicAuthUser(this, username, principal.getRoles())));
                return;
            }
        }

        logger.warn("Failed authentication");
        resultHandler.handle(Future.failedFuture("Failed authentication"));
        return;
    }

    /**
     * Authorizes access the protected resources.
     *
     * @param username the user name
     * @param resourceRole the required role for resource access
     * @param resultHandler handler for authorization result
     */
    public void authorize(String username, String resourceRole, Handler<AsyncResult<Boolean>> resultHandler) {
        if (!serverAuthDataConfig.isAuthenticationEnabled()) {
            throw new IllegalStateException("BasicAuthentication is not enabled.");
        }
        boolean authorized = false;
        for (Principal principal : serverAuthDataConfig.getPrincipals()) {
            logger.debug(principal);
            if (principal.getUsername().equals(username)) {
                List<String> assignedUserRoles = Arrays.asList(principal.getRoles().split("\\s*,\\s*"));
                logger.debug(
                        "ResourceRole = {0}, User {1} has assigned roles {2}", resourceRole, username, assignedUserRoles
                );
                if (assignedUserRoles.contains(GPConstants.ADMIN_ROLE) || assignedUserRoles.contains(resourceRole)) {
                    logger.debug(
                            "User {0} is allowed access to resource protected by role {1}", username, resourceRole
                    );
                    authorized = true;
                } else {
                    logger.info(
                            "User {0} is not allowed access to resource protected by role {1}",
                            username, resourceRole
                    );
                }
                resultHandler.handle(Future.succeededFuture(authorized));
                return;
            }
        }
    }

    public ServerAuthDataConfiguration getServerAuthDataConfiguration() {
        return this.serverAuthDataConfig;
    }
}
