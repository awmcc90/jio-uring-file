package io.jiouring.benchmark;

import com.sun.nio.file.ExtendedOpenOption;
import io.jiouring.file.IoUringFileIoHandle;
import io.netty.buffer.ByteBuf;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(
    value = 1,
    jvmArgs = {
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--enable-native-access=ALL-UNNAMED",
        "-Xms1G",
        "-Xmx1G",
        "-XX:+AlwaysPreTouch",
        "-XX:MaxDirectMemorySize=2G",
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Dio.netty.iouring.ringSize=4096",
        "-Dio.netty.noUnsafe=false",
        "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
    }
)
public class RandomWriteBenchmark {

    private static final Logger logger = LogManager.getLogger(RandomWriteBenchmark.class);

    private MultiThreadIoEventLoopGroup group;
    private IoUringFileIoHandle ioUringFile;
    private FileChannel channel;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        Path path = Files.createTempFile("bench_write", ".dat");
        path.toFile().deleteOnExit();

        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{0}), 1024L * 1024L * 1024L - 1);
            ch.force(true);
        }

        group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());

        ioUringFile = IoUringFileIoHandle.open(
            path,
            group.next(),
            new OpenOption[]{
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                ExtendedOpenOption.DIRECT
            }
        ).join();

        channel = FileChannel.open(
            path,
            StandardOpenOption.WRITE,
            ExtendedOpenOption.DIRECT
        );
    }

    @Benchmark
    public void ioUring_random_write(RandomIoState.Write state) throws Exception {
        Future[] futures = state.futures;
        ByteBuf[] buffers = state.buffers;
        long[] offsets = state.randomOffsets;
        int batchSize = state.batchSize;

        for (int i = 0; i < batchSize; i++) {
            futures[i] = ioUringFile.writeAsync(buffers[i].retain(), offsets[i], false);
        }

        for (int i = 0; i < batchSize; i++) {
            futures[i].await();
            buffers[i].release();
        }
    }

    @Benchmark
    public void fileChannel_random_write(RandomIoState.Write state) throws Exception {
        ByteBuffer[] nioBuffers = state.nioBuffers;
        long[] offsets = state.randomOffsets;
        int batchSize = state.batchSize;

        for (int i = 0; i < batchSize; i++) {
            channel.write(nioBuffers[i], offsets[i]);
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        try {
            if (channel != null && channel.isOpen()) {
                channel.force(false);
            }
        } catch (Exception e) {
            logger.error("Final sync failed", e);
        } finally {
            if (channel != null) channel.close();
            if (ioUringFile != null) ioUringFile.closeAsync().syncUninterruptibly();
            if (group != null) group.shutdownGracefully().syncUninterruptibly();
        }
    }
}