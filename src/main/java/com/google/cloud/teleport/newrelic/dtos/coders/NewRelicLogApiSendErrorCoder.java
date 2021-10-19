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
 *  * deserializing {@link NewRelicLogApiSendError} objects, which can then be transmitted through a Beam pipeline using
 *  * PCollection/PCollectionTuple objects.
 */
public class NewRelicLogApiSendErrorCoder extends AtomicCoder<NewRelicLogApiSendError> {

    private static final NewRelicLogApiSendErrorCoder NewRelic_WRITE_ERROR_CODER = new NewRelicLogApiSendErrorCoder();
    private static final TypeDescriptor<NewRelicLogApiSendError> TYPE_DESCRIPTOR = new TypeDescriptor<NewRelicLogApiSendError>() {};
    private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
    private static final NullableCoder<String> STRING_NULLABLE_CODER = NullableCoder.of(STRING_UTF_8_CODER);
    private static final NullableCoder<Integer> INTEGER_NULLABLE_CODER = NullableCoder.of(BigEndianIntegerCoder.of());

    public static NewRelicLogApiSendErrorCoder of() {
        return NewRelic_WRITE_ERROR_CODER;
    }

    @Override
    public void encode(NewRelicLogApiSendError value, OutputStream out) throws IOException {
        INTEGER_NULLABLE_CODER.encode(value.getStatusCode(), out);
        STRING_NULLABLE_CODER.encode(value.getStatusMessage(), out);
        STRING_NULLABLE_CODER.encode(value.getPayload(), out);
    }

    @Override
    public NewRelicLogApiSendError decode(InputStream in) throws IOException {

        NewRelicLogApiSendError error = new NewRelicLogApiSendError();

        error.setStatusCode(INTEGER_NULLABLE_CODER.decode(in));
        error.setStatusMessage(STRING_NULLABLE_CODER.decode(in));
        error.setPayload(STRING_NULLABLE_CODER.decode(in));

        return error;
    }

    @Override
    public TypeDescriptor<NewRelicLogApiSendError> getEncodedTypeDescriptor() {
        return TYPE_DESCRIPTOR;
    }

    @Override
    public void verifyDeterministic() throws Coder.NonDeterministicException {
        throw new Coder.NonDeterministicException(
                this, "NewRelicWriteError can hold arbitrary instances, which may be non-deterministic.");
    }
}

