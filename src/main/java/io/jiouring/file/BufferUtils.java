package io.jiouring.file;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;

import java.nio.ByteBuffer;

public class BufferUtils {

    private BufferUtils() {}

    // TODO: Initialize this based on machine
    private static final int ALIGNMENT = 4096;

    // Necessary for O_DIRECT
    public static ByteBuf alignedByteBuf(int size) {
        int totalSize = size + ALIGNMENT;

        ByteBuffer raw = ByteBuffer.allocateDirect(totalSize);
        long baseAddress = PlatformDependent.directBufferAddress(raw);

        long remainder = baseAddress % ALIGNMENT;
        int offset = (remainder == 0) ? 0 : (int) (ALIGNMENT - remainder);

        raw.position(offset);
        raw.limit(offset + size);
        ByteBuffer alignedSlice = raw.slice();

        while (alignedSlice.hasRemaining()) alignedSlice.put((byte) 1);
        alignedSlice.clear();

        return Unpooled.wrappedBuffer(alignedSlice);
    }
}