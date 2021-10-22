package com.google.cloud.teleport.newrelic.dtos;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Optional;

/**
 * A class representing a New Relic Log record.
 */
public class NewRelicLogRecord {

    /** Message, can either be a plain text string or a string representing a JSON object. Mandatory. */
    private String message;
    /** Timestamp of the log record */
    private Optional<Long> timestamp;

    public NewRelicLogRecord(final String message, final Long timestamp) {
        this.message = message;
        this.timestamp = Optional.ofNullable(timestamp);
    }

    public Optional<Long> getTimestamp() {
        return timestamp;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        NewRelicLogRecord that = (NewRelicLogRecord) o;

        return new EqualsBuilder()
                .append(message, that.message)
                .append(timestamp, that.timestamp)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(message)
                .append(timestamp)
                .toHashCode();
    }
}