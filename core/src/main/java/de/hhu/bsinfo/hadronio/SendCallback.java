package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxSendCallback;
import de.hhu.bsinfo.hadronio.generated.DebugConfig;
import de.hhu.bsinfo.hadronio.util.RingBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class SendCallback implements UcxSendCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendCallback.class);

    private final RingBuffer sendBuffer;
    private final AtomicBoolean padding = new AtomicBoolean();
    private final ReadHandler readHandler = new ReadHandler(padding);

    public SendCallback(RingBuffer sendBuffer) {
        this.sendBuffer = sendBuffer;
    }

    @Override
    public void onMessageSent() {
        if (DebugConfig.DEBUG) LOGGER.debug("hadroNIO SendCallback called");
        padding.set(true);
        int readFromBuffer;

        do {
            readFromBuffer = sendBuffer.read(readHandler, 1);

            if (padding.get()) {
                if (DebugConfig.DEBUG) LOGGER.debug("Read [{}] padding bytes from send buffer", readFromBuffer);
                sendBuffer.commitRead(readFromBuffer);
            }
        } while (padding.get());

        sendBuffer.commitRead(readFromBuffer);
    }

    private static final class ReadHandler implements MessageHandler {

        private final AtomicBoolean padding;

        private ReadHandler(AtomicBoolean padding) {
            this.padding = padding;
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
            if (DebugConfig.DEBUG) LOGGER.debug("Message type id: [{}], Index: [{}], Length: [{}]", msgTypeId, index, length);
            padding.set(false);
        }
    }
}
