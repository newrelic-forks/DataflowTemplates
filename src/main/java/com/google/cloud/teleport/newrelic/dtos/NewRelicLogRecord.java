package com.google.cloud.teleport.newrelic.dtos;

/**
 * A class representing a New Relic Log record.
 */
public class NewRelicLogRecord {

    /** Timestamp of the log record (optional) */
    private Long timestamp;
    /** Message, can either be a plain text string or a string representing a JSON object. Mandatory. */
    private String message;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}