package org.prebid.pg.gp.server.exception;

/**
 * Unchecked exceptions thrown for invalid user request.
 */
public class InvalidRequestException extends RuntimeException {

    private final String code;
    private final String exception;

    public InvalidRequestException(String code, String exception) {
        super(exception);
        this.exception = exception;
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public String getException() {
        return exception;
    }

}
