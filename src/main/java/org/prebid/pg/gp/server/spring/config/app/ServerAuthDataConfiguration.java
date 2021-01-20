package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Configuration properties for authentication.
 */

@Configuration
@Data
@ToString(exclude = {"password"})
@ConfigurationProperties(prefix = "server-auth")
public class ServerAuthDataConfiguration {

    List<Principal> principals;

    boolean authenticationEnabled;

    @Data
    public static class Principal {

        @NotNull
        String username;

        @NotNull
        String password;

        @NotNull
        String roles;

        public String toString() {
            return String.format("username=%s, roles=%s", username, roles);
        }

    }

}
