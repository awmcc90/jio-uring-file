package io.jiouring.file;

public final class NativeConstants {

    private NativeConstants() {}

    public static final class IoRingOp {
        private IoRingOp() {}

        public static final byte NOP             = 0;
        public static final byte READV           = 1;
        public static final byte WRITEV          = 2;
        public static final byte FSYNC           = 3;
        public static final byte READ_FIXED      = 4;
        public static final byte WRITE_FIXED     = 5;
        public static final byte POLL_ADD        = 6;
        public static final byte POLL_REMOVE     = 7;
        public static final byte SYNC_FILE_RANGE = 8;
        public static final byte SENDMSG         = 9;
        public static final byte RECVMSG         = 10;
        public static final byte TIMEOUT         = 11;
        public static final byte TIMEOUT_REMOVE  = 12;
        public static final byte ACCEPT          = 13;
        public static final byte ASYNC_CANCEL    = 14;
        public static final byte LINK_TIMEOUT    = 15;
        public static final byte CONNECT         = 16;
        public static final byte FALLOCATE       = 17;
        public static final byte OPENAT          = 18;
        public static final byte CLOSE           = 19;
        public static final byte FILES_UPDATE    = 20;
        public static final byte STATX           = 21;
        public static final byte READ            = 22;
        public static final byte WRITE           = 23;
        public static final byte FADVISE         = 24;
        public static final byte MADVISE         = 25;
        public static final byte SPLICE          = 28;
        public static final byte TEE             = 33;
        public static final byte SHUTDOWN        = 34;
        public static final byte RENAMEAT        = 35;
        public static final byte UNLINKAT        = 36;
        public static final byte MKDIRAT         = 37;
        public static final byte FTRUNCATE       = 46;
    }

    public static final class OpenFlags {
        private OpenFlags() {}

        public static final int RDONLY    = 0x00000000;
        public static final int WRONLY    = 0x00000001;
        public static final int RDWR      = 0x00000002;

        public static final int CREAT     = 0x00000040;
        public static final int EXCL      = 0x00000080;
        public static final int NOCTTY    = 0x00000100;
        public static final int TRUNC     = 0x00000200;
        public static final int APPEND    = 0x00000400;
        public static final int NONBLOCK  = 0x00000800;

        public static final int DSYNC     = 0x00001000;
        public static final int ASYNC     = 0x00002000;
        public static final int DIRECT    = 0x00004000;
        public static final int LARGEFILE = 0x00008000;
        public static final int DIRECTORY = 0x00010000;
        public static final int NOFOLLOW  = 0x00020000;
        public static final int NOATIME   = 0x00040000;
        public static final int CLOEXEC   = 0x00080000;
        public static final int SYNC      = 0x00100000 | DSYNC;
        public static final int PATH      = 0x00200000;
        public static final int TMPFILE   = 0x00400000 | DIRECTORY;
    }

    public static final class FileMode {
        private FileMode() {}

        public static final int S_IRUSR = 0x100;
        public static final int S_IWUSR = 0x080;
        public static final int S_IXUSR = 0x040;
        public static final int S_IRWXU = 0x1C0;

        public static final int S_IRGRP = 0x020;
        public static final int S_IWGRP = 0x010;
        public static final int S_IXGRP = 0x008;
        public static final int S_IRWXG = 0x038;

        public static final int S_IROTH = 0x004;
        public static final int S_IWOTH = 0x002;
        public static final int S_IXOTH = 0x001;
        public static final int S_IRWXO = 0x007;

        public static final int DEFAULT_FILE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        public static final int DEFAULT_DIR  = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
    }

    public static final class RwFlags {
        private RwFlags() {}

        public static final int HIPRI  = 0x00000001;
        public static final int DSYNC  = 0x00000002;
        public static final int SYNC   = 0x00000004;
        public static final int NOWAIT = 0x00000008;
        public static final int APPEND = 0x00000010;
    }

    public static final class FsyncFlags {
        private FsyncFlags() {}

        public static final int DATASYNC = 1;
    }

    public static final class AsyncCancelFlags {
        private AsyncCancelFlags() {}

        public static final int ALL = 1;
        public static final int FD  = 2;
        public static final int ANY = 4;
    }

    public static final class AtFlags {
        private AtFlags() {}

        public static final int AT_REMOVEDIR  = 0x0200;
        public static final int AT_EMPTY_PATH = 0x1000;
    }

    public static class FallocateFlags {
        public static final int KEEP_SIZE      = 0x01;
        public static final int PUNCH_HOLE     = 0x02;
        public static final int NO_HIDE_STALE  = 0x04;
        public static final int COLLAPSE_RANGE = 0x08;
        public static final int ZERO_RANGE     = 0x10;
        public static final int INSERT_RANGE   = 0x20;
        public static final int UNSHARE_RANGE  = 0x40;
    }

    public static class StatxMask {
        public static final int TYPE        = 0x00000001;
        public static final int MODE        = 0x00000002;
        public static final int NLINK       = 0x00000004;
        public static final int UID         = 0x00000008;
        public static final int GID         = 0x00000010;
        public static final int ATIME       = 0x00000020;
        public static final int MTIME       = 0x00000040;
        public static final int CTIME       = 0x00000080;
        public static final int INO         = 0x00000100;
        public static final int SIZE        = 0x00000200;
        public static final int BLOCKS      = 0x00000400;
        public static final int BASIC_STATS = 0x000007ff;
        public static final int BTIME       = 0x00000800;
        public static final int ALL         = 0x00000fff;
    }

    public static class StatxFlags {
        public static final int AT_SYNC_AS_STAT     = 0x0000;
        public static final int AT_FORCE_SYNC       = 0x2000;
        public static final int AT_DONT_SYNC        = 0x4000;
        public static final int AT_SYMLINK_NOFOLLOW = 0x0100;
    }
}