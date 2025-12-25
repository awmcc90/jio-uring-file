package io.jiouring.file;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class Buffers {

    // TODO: It's not always this...
    private static final int ALIGNMENT = 4096;

    private Buffers() {}

    public static ByteBuf direct(int size) {
        return Unpooled.directBuffer(size)
            .order(ByteOrder.nativeOrder())
            .setZero(0, size);
    }

    public static ByteBuf direct(byte[] data) {
        return direct(data.length).writeBytes(data);
    }

    public static ByteBuf pooledDirect(int size) {
        return PooledByteBufAllocator.DEFAULT.directBuffer(size)
            .order(ByteOrder.nativeOrder())
            .setZero(0, size);
    }

    public static ByteBuf pooledDirect(byte[] data) {
        return pooledDirect(data.length).writeBytes(data);
    }

    // Necessary for O_DIRECT
    public static ByteBuf alignedByteBuf(int size) {
        int totalSize = size + ALIGNMENT;

        ByteBuffer raw = ByteBuffer.allocateDirect(totalSize)
            .order(ByteOrder.nativeOrder());
        long baseAddress = PlatformDependent.directBufferAddress(raw);

        long remainder = baseAddress % ALIGNMENT;
        int offset = (remainder == 0) ? 0 : (int) (ALIGNMENT - remainder);

        raw.position(offset);
        raw.limit(offset + size);
        ByteBuffer alignedSlice = raw.slice();

        while (alignedSlice.hasRemaining()) alignedSlice.put((byte) 1);
        alignedSlice.clear();

        return Unpooled.wrappedBuffer(alignedSlice)
            .order(ByteOrder.nativeOrder());
    }
}