package org.prebid.pg.gp.server.exception;

/**
 * Unchecked exceptions thrown in the general planner.
 */
public class GeneralPlannerException extends RuntimeException {

    public GeneralPlannerException(String message) {
        super(message);
    }

    public GeneralPlannerException(Throwable cause) {
        super(cause);
    }

    public GeneralPlannerException(String message, Throwable cause) {
        super(message, cause);
    }
}
