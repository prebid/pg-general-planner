package org.prebid.pg.gp.server.spring.config;

import org.prebid.pg.gp.server.model.AdminTracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for administration related objects.
 */

@Configuration
public class AdminConfiguration {

    @Bean
    AdminTracer adminTracer() {
        return new AdminTracer();
    }

}
