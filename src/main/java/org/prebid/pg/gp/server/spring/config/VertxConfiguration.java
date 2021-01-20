package org.prebid.pg.gp.server.spring.config;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for vertx.
 */

@Configuration
class VertxConfiguration {

    @Bean
    Vertx vertx(@Value("${vertx.worker-pool-size:20}") int workerPoolSize) {
        return Vertx.vertx(new VertxOptions().setWorkerPoolSize(workerPoolSize));
    }

}

