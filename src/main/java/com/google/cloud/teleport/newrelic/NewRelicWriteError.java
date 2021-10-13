package com.google.cloud.teleport.newrelic;

/**
 * A class for capturing errors writing {@link NewRelicEvent}s to NewRelic's Log API end point.
 */
public class NewRelicWriteError {

    private Integer statusCode;
    private String statusMessage;
    private String payload;

    public Integer getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
