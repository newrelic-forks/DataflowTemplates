package com.google.cloud.teleport.newrelic;

/**
 * A class for NewRelic events.
 */
public class NewRelicEvent {

    private Long timestamp;
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