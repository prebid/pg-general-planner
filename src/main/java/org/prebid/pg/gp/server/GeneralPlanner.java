package org.prebid.pg.gp.server;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.util.Constants;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.Map;

/**
 * Main entry point of the application.
 */

@SuppressWarnings("checkstyle:hideutilityclassconstructor")
@SpringBootApplication
public class GeneralPlanner {

    private static final Logger logger = LoggerFactory.getLogger(GeneralPlanner.class);

    public static void main(String[] args) {
        logger.info("Starting application ...");
        Map<String, String> env = System.getenv();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            String envName = entry.getKey();
            if (!envName.toLowerCase().contains("password")) {
                logger.info("{0} == {1}", envName, entry.getValue());
            }
        }
        ApplicationContext ctx = SpringApplication.run(GeneralPlanner.class, args);
        logger.debug("Application ready");
        ctx.getBean(AlertProxyHttpClient.class)
                .raiseEvent(Constants.GP_START_UP, AlertPriority.NOTIFICATION, "GeneralPlanner started");
        logger.info("Application ready, Start up notice prepared");
    }
}
