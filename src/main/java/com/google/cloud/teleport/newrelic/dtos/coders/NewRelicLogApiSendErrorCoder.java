package com.google.cloud.teleport.newrelic.dtos.coders;

import com.google.cloud.teleport.newrelic.dtos.NewRelicLogApiSendError;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link org.apache.beam.sdk.coders.Coder} for {@link NewRelicLogApiSendError} objects. It allows serializing and
 * deserializing {@link NewRelicLogApiSendError} objects, which can then be transmitted through a Beam pipeline using
 * PCollection/PCollectionTuple objects.
 */
public class NewRelicLogApiSendErrorCoder extends AtomicCoder<NewRelicLogApiSendError> {

    private static final NewRelicLogApiSendErrorCoder SINGLETON = new NewRelicLogApiSendErrorCoder();
    private static final TypeDescriptor<NewRelicLogApiSendError> TYPE_DESCRIPTOR = new TypeDescriptor<NewRelicLogApiSendError>() {};
    private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
    private static final NullableCoder<String> STRING_NULLABLE_CODER = NullableCoder.of(STRING_UTF_8_CODER);
    private static final NullableCoder<Integer> INTEGER_NULLABLE_CODER = NullableCoder.of(BigEndianIntegerCoder.of());

    public static NewRelicLogApiSendErrorCoder getInstance() {
        return SINGLETON;
    }

    @Override
    public void encode(NewRelicLogApiSendError value, OutputStream out) throws IOException {
        INTEGER_NULLABLE_CODER.encode(value.getStatusCode().orElse(null), out);
        STRING_NULLABLE_CODER.encode(value.getStatusMessage(), out);
        STRING_NULLABLE_CODER.encode(value.getPayload(), out);
    }

    @Override
    public NewRelicLogApiSendError decode(InputStream in) throws IOException {
        final Integer statusCode = INTEGER_NULLABLE_CODER.decode(in);
        final String statusMessage = STRING_NULLABLE_CODER.decode(in);
        final String payload = STRING_NULLABLE_CODER.decode(in);

        return new NewRelicLogApiSendError(payload, statusMessage, statusCode);
    }

    @Override
    public TypeDescriptor<NewRelicLogApiSendError> getEncodedTypeDescriptor() {
        return TYPE_DESCRIPTOR;
    }

    @Override
    public void verifyDeterministic() throws Coder.NonDeterministicException {
        throw new Coder.NonDeterministicException(
                this, "NewRelicLogApiSendError can hold arbitrary instances, which may be non-deterministic.");
    }
}

