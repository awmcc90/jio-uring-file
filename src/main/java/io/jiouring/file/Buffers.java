package io.jiouring.file;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;

import java.nio.ByteOrder;

public final class Buffers {

    // TODO: It's not always this...
    private static final int ALIGNMENT = 4096;

    private Buffers() {}

    private static ByteBuf direct(ByteBufAllocator allocator, int size, boolean zero) {
        ByteBuf bb = allocator.directBuffer(size).order(ByteOrder.nativeOrder());
        if (zero) bb.setZero(0, size);
        return bb;
    }

    public static ByteBuf direct(int size, boolean zero) {
        return direct(ByteBufAllocator.DEFAULT, size, zero);
    }

    public static ByteBuf direct(int size) {
        return direct(ByteBufAllocator.DEFAULT, size, false);
    }

    public static ByteBuf direct(byte[] data) {
        return direct(data.length).writeBytes(data);
    }

    public static ByteBuf direct() {
        return ByteBufAllocator.DEFAULT.directBuffer();
    }

    public static ByteBuf unpooledDirect(int size, boolean zero) {
        return direct(UnpooledByteBufAllocator.DEFAULT, size, zero);
    }

    public static ByteBuf unpooledDirect(int size) {
        return unpooledDirect(size, false);
    }

    public static ByteBuf unpooledDirect(byte[] data) {
        return unpooledDirect(data.length).writeBytes(data);
    }

    public static ByteBuf pooledDirect(int size, boolean zero) {
        return direct(PooledByteBufAllocator.DEFAULT, size, zero);
    }

    public static ByteBuf pooledDirect(int size) {
        return pooledDirect(size, false);
    }

    public static ByteBuf pooledDirect(byte[] data) {
        return pooledDirect(data.length).writeBytes(data);
    }

    public static ByteBuf pooledDirect() {
        return PooledByteBufAllocator.DEFAULT.directBuffer();
    }

    private static ByteBuf alignedDirect(ByteBufAllocator allocator, int size, int alignment, boolean zero) {
        if (alignment <= 0 || (alignment & (alignment - 1)) != 0) {
            throw new IllegalArgumentException("alignment must be a power of two");
        }

        ByteBuf bb = allocator.directBuffer(size + alignment);

        try {
            long base = bb.memoryAddress();
            int offset = (int) -base & (alignment - 1);

            ByteBuf aligned = bb.retainedSlice(offset, size).order(ByteOrder.nativeOrder());
            bb.release();
            aligned.writerIndex(0);

            if (zero) PlatformDependent.setMemory(base + offset, size, (byte) 0);

            return aligned;
        } catch (Throwable t) {
            bb.release();
            throw t;
        }
    }

    private static ByteBuf alignedDirect(ByteBufAllocator allocator, int size, int alignment) {
        return alignedDirect(allocator, size, alignment, false);
    }

    public static ByteBuf alignedDirect(int size, boolean zero) {
        return alignedDirect(ByteBufAllocator.DEFAULT, size, ALIGNMENT, zero);
    }

    public static ByteBuf alignedDirect(int size) {
        return alignedDirect(ByteBufAllocator.DEFAULT, size, ALIGNMENT);
    }

    public static ByteBuf unpooledAlignedDirect(int size) {
        return alignedDirect(UnpooledByteBufAllocator.DEFAULT, size, ALIGNMENT);
    }

    public static ByteBuf pooledAlignedDirect(int size, boolean zero) {
        return alignedDirect(PooledByteBufAllocator.DEFAULT, size, ALIGNMENT, zero);
    }

    public static ByteBuf pooledAlignedDirect(int size) {
        return alignedDirect(PooledByteBufAllocator.DEFAULT, size, ALIGNMENT);
    }
}