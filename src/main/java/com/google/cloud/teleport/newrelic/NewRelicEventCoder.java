/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.newrelic;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link org.apache.beam.sdk.coders.Coder} for {@link NewRelicEvent} objects.
 */
public class NewRelicEventCoder extends AtomicCoder<NewRelicEvent> {

    private static final NewRelicEventCoder EVENT_CODER = new NewRelicEventCoder();
    private static final TypeDescriptor<NewRelicEvent> TYPE_DESCRIPTOR = new TypeDescriptor<NewRelicEvent>() {};
    private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
    private static final NullableCoder<Long> LONG_NULLABLE_CODER = NullableCoder.of(BigEndianLongCoder.of());

    public static NewRelicEventCoder of() {
        return EVENT_CODER;
    }

    @Override
    public void encode(NewRelicEvent value, OutputStream out) throws IOException {
        LONG_NULLABLE_CODER.encode(value.getTimestamp(), out);
        STRING_UTF_8_CODER.encode(value.getMessage(), out);
    }

    @Override
    public NewRelicEvent decode(InputStream in) throws IOException {

        NewRelicEvent nrEvent = new NewRelicEvent();

        Long time = LONG_NULLABLE_CODER.decode(in);
        if (time != null) {
            nrEvent.setTimestamp(time);
        }

        String msg = STRING_UTF_8_CODER.decode(in);
        nrEvent.setMessage(msg);

        return nrEvent;
    }

    @Override
    public TypeDescriptor<NewRelicEvent> getEncodedTypeDescriptor() {
        return TYPE_DESCRIPTOR;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        throw new NonDeterministicException(
                this, "NewRelicEvent can hold arbitrary instances, which may be non-deterministic.");
    }
}
