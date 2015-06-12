package com.yeahmobi.datasystem.query.exception;

public class NullBRPException extends Exception {
    private static final long serialVersionUID = -6599318638125353754L;

    public NullBRPException() {
        super();
    }

    public NullBRPException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public NullBRPException(String message, Throwable cause) {
        super(message, cause);
    }

    public NullBRPException(String message) {
        super(message);
    }

    public NullBRPException(Throwable cause) {
        super(cause);
    }
    

}
