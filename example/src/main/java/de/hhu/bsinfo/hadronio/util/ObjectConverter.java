package de.hhu.bsinfo.hadronio.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.Output;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ObjectConverter {

    private static final int INITIAL_BUFFER_SIZE = 1024;

    private static final int MAX_BUFFER_SIZE = -1;

    private static final ThreadLocal<Kryo> LOCAL_KRYO = ThreadLocal.withInitial(ObjectConverter::newKryo);
    private static final ThreadLocal<Output> LOCAL_OUTPUT = ThreadLocal.withInitial(ObjectConverter::newOutput);
    private static final ThreadLocal<ByteBufferInput> LOCAL_INPUT = ThreadLocal.withInitial(ObjectConverter::newInput);

    public ByteBuffer serialize(Object object) {
        final var kryo = LOCAL_KRYO.get();
        final var output = LOCAL_OUTPUT.get();

        kryo.writeClassAndObject(output, object);
        final var result = ByteBuffer.wrap(Arrays.copyOfRange(output.getBuffer(), 0, output.position()));
        output.reset();

        return result;
    }

    @SuppressWarnings("unchecked")
    public <T> T deserialize(ByteBuffer buffer) {
        final var kryo = LOCAL_KRYO.get();
        final var input = LOCAL_INPUT.get();

        input.setBuffer(buffer);
        return (T) kryo.readClassAndObject(input);
    }

    private static Kryo newKryo() {
        final var kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        return kryo;
    }

    private static Output newOutput() {
        return new Output(INITIAL_BUFFER_SIZE, MAX_BUFFER_SIZE);
    }

    private static ByteBufferInput newInput() {
        return new ByteBufferInput();
    }

}
