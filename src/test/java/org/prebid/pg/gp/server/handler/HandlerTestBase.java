package org.prebid.pg.gp.server.handler;

import org.prebid.pg.gp.server.auth.BasicAuthProvider;
import org.prebid.pg.gp.server.spring.config.app.ServerAuthDataConfiguration;

import java.util.ArrayList;
import java.util.List;

public class HandlerTestBase {

    protected BasicAuthProvider getBasicAuthProvider(String roles, String user, String password) {
        ServerAuthDataConfiguration serverAuthDataConfiguration = new ServerAuthDataConfiguration();
        serverAuthDataConfiguration.setAuthenticationEnabled(true);

        List<ServerAuthDataConfiguration.Principal> principals = new ArrayList<>();
        ServerAuthDataConfiguration.Principal principal = new ServerAuthDataConfiguration.Principal();
        principal.setUsername(user);
        principal.setPassword(password);
        principal.setRoles(roles);
        principals.add(principal);
        serverAuthDataConfiguration.setPrincipals(principals);

        return new BasicAuthProvider(serverAuthDataConfiguration);
    }

}
