package io.jiouring.benchmark;

import io.jiouring.file.Buffers;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Thread)
public class RandomIoState {

    @Param("1024")
    public int batchSize;

    @Param("4096")
    public int bufferSize;

    public final long fileSize = 1024L * 1024L * 1024L; // 1GB

    public long[] randomOffsets;
    public ByteBuf[] buffers;
    public ByteBuffer[] nioBuffers;
    public Future[] futures;

    @Setup(Level.Trial)
    public void setup() {
        futures = new Future[batchSize];
        buffers = new ByteBuf[batchSize];
        nioBuffers = new ByteBuffer[batchSize];
        randomOffsets = new long[batchSize];

        long maxBlockIndex = fileSize / 4096;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < batchSize; i++) {
            buffers[i] = Buffers.alignedDirect(bufferSize);
            nioBuffers[i] = buffers[i].nioBuffer();

            // Random aligned offset
            randomOffsets[i] = rnd.nextLong(maxBlockIndex) * 4096;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (buffers != null) {
            for (ByteBuf buf : buffers) {
                if (buf != null) {
                    buf.release();
                }
            }
        }
    }

    protected void shuffleOffsets() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = batchSize - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            long temp = randomOffsets[index];
            randomOffsets[index] = randomOffsets[i];
            randomOffsets[i] = temp;
        }
    }

    public static class Read extends RandomIoState {

        @Setup(Level.Invocation)
        public void reset() {
            for (int i = 0; i < batchSize; i++) {
                buffers[i].clear();
                nioBuffers[i].clear();
            }
            shuffleOffsets();
        }
    }

    public static class Write extends RandomIoState {

        @Setup(Level.Invocation)
        public void reset() {
            for (int i = 0; i < batchSize; i++) {
                buffers[i].setIndex(0, bufferSize);
                nioBuffers[i].clear();
            }
            shuffleOffsets();
        }
    }
}