package com.google.cloud.teleport.newrelic;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link org.apache.beam.sdk.coders.Coder} for {@link NewRelicEvent} objects.
 */
public class NewRelicWriteErrorCoder extends AtomicCoder<NewRelicWriteError> {

        private static final NewRelicWriteErrorCoder NewRelic_WRITE_ERROR_CODER
                = new NewRelicWriteErrorCoder();

        private static final TypeDescriptor<NewRelicWriteError> TYPE_DESCRIPTOR =
                new TypeDescriptor<NewRelicWriteError>() {
                };
        private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
        private static final NullableCoder<String> STRING_NULLABLE_CODER =
                NullableCoder.of(STRING_UTF_8_CODER);
        private static final NullableCoder<Integer> INTEGER_NULLABLE_CODER =
                NullableCoder.of(BigEndianIntegerCoder.of());

        public static NewRelicWriteErrorCoder of () {
        return NewRelic_WRITE_ERROR_CODER;
    }

        @Override
        public void encode (NewRelicWriteError value, OutputStream out) throws IOException {
        INTEGER_NULLABLE_CODER.encode(value.getStatusCode(), out);
        STRING_NULLABLE_CODER.encode(value.getStatusMessage(), out);
        STRING_NULLABLE_CODER.encode(value.getPayload(), out);
    }

        @Override
        public NewRelicWriteError decode (InputStream in) throws IOException {

        NewRelicWriteError error = new NewRelicWriteError();

        error.setStatusCode(INTEGER_NULLABLE_CODER.decode(in));
        error.setStatusMessage(STRING_NULLABLE_CODER.decode(in));
        error.setPayload(STRING_NULLABLE_CODER.decode(in));

        return error;
    }

        @Override
        public TypeDescriptor<NewRelicWriteError> getEncodedTypeDescriptor () {
        return TYPE_DESCRIPTOR;
    }

        @Override
        public void verifyDeterministic () throws Coder.NonDeterministicException {
        throw new Coder.NonDeterministicException(
                this, "NewRelicWriteError can hold arbitrary instances, which may be non-deterministic.");
    }
}

