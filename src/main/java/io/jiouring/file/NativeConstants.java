package io.jiouring.file;

public final class NativeConstants {

    private NativeConstants() {}

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/io_uring.h#L240
    public static final class IoUringOp {
        private IoUringOp() {}

        public static final byte IORING_OP_NOP                 =  0;
        public static final byte IORING_OP_READV               =  1;
        public static final byte IORING_OP_WRITEV              =  2;
        public static final byte IORING_OP_FSYNC               =  3;
        public static final byte IORING_OP_READ_FIXED          =  4;
        public static final byte IORING_OP_WRITE_FIXED         =  5;
        public static final byte IORING_OP_POLL_ADD            =  6;
        public static final byte IORING_OP_POLL_REMOVE         =  7;
        public static final byte IORING_OP_SYNC_FILE_RANGE     =  8;
        public static final byte IORING_OP_SENDMSG             =  9;
        public static final byte IORING_OP_RECVMSG             = 10;
        public static final byte IORING_OP_TIMEOUT             = 11;
        public static final byte IORING_OP_TIMEOUT_REMOVE      = 12;
        public static final byte IORING_OP_ACCEPT              = 13;
        public static final byte IORING_OP_ASYNC_CANCEL        = 14;
        public static final byte IORING_OP_LINK_TIMEOUT        = 15;
        public static final byte IORING_OP_CONNECT             = 16;
        public static final byte IORING_OP_FALLOCATE           = 17;
        public static final byte IORING_OP_OPENAT              = 18;
        public static final byte IORING_OP_CLOSE               = 19;
        public static final byte IORING_OP_FILES_UPDATE        = 20;
        public static final byte IORING_OP_STATX               = 21;
        public static final byte IORING_OP_READ                = 22;
        public static final byte IORING_OP_WRITE               = 23;
        public static final byte IORING_OP_FADVISE             = 24;
        public static final byte IORING_OP_MADVISE             = 25;
        public static final byte IORING_OP_SEND                = 26;
        public static final byte IORING_OP_RECV                = 27;
        public static final byte IORING_OP_OPENAT2             = 28;
        public static final byte IORING_OP_EPOLL_CTL           = 29;
        public static final byte IORING_OP_SPLICE              = 30;
        public static final byte IORING_OP_PROVIDE_BUFFERS     = 31;
        public static final byte IORING_OP_REMOVE_BUFFERS      = 32;
        public static final byte IORING_OP_TEE                 = 33;
        public static final byte IORING_OP_SHUTDOWN            = 34;
        public static final byte IORING_OP_RENAMEAT            = 35;
        public static final byte IORING_OP_UNLINKAT            = 36;
        public static final byte IORING_OP_MKDIRAT             = 37;
        public static final byte IORING_OP_SYMLINKAT           = 38;
        public static final byte IORING_OP_LINKAT              = 39;
        public static final byte IORING_OP_MSG_RING            = 40;
        public static final byte IORING_OP_FSETXATTR           = 41;
        public static final byte IORING_OP_SETXATTR            = 42;
        public static final byte IORING_OP_FGETXATTR           = 43;
        public static final byte IORING_OP_GETXATTR            = 44;
        public static final byte IORING_OP_SOCKET              = 45;
        public static final byte IORING_OP_URING_CMD           = 46;
        public static final byte IORING_OP_SEND_ZC             = 47;
        public static final byte IORING_OP_SENDMSG_ZC          = 48;
        public static final byte IORING_OP_READ_MULTISHOT      = 49;
        public static final byte IORING_OP_WAITID              = 50;
        public static final byte IORING_OP_FUTEX_WAIT          = 51;
        public static final byte IORING_OP_FUTEX_WAKE          = 52;
        public static final byte IORING_OP_FUTEX_WAITV         = 53;
        public static final byte IORING_OP_FIXED_FD_INSTALL    = 54;
        public static final byte IORING_OP_FTRUNCATE           = 55;
        public static final byte IORING_OP_BIND                = 56;
        public static final byte IORING_OP_LISTEN              = 57;
        public static final byte IORING_OP_RECV_ZC             = 58;
        public static final byte IORING_OP_EPOLL_WAIT          = 59;
        public static final byte IORING_OP_READV_FIXED         = 60;
        public static final byte IORING_OP_WRITEV_FIXED        = 61;
        public static final byte IORING_OP_PIPE                = 62;

        // Doesn't map to IORING_OP_LAST. This is just a marker to reference in OpIdPool. And it's here so
        // that I remember to update it when new ops are added.
        public static final byte _LAST = 63;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/asm-generic/fcntl.h#L18
    public static final class OpenFlags {
        private OpenFlags() {}

        // Access mode mask
        public static final int O_RDONLY  = 0x0000;
        public static final int O_WRONLY  = 0x0001;
        public static final int O_RDWR    = 0x0002;
        public static final int O_ACCMODE = 0x0003;

        // Creation and status flags
        public static final int O_CREAT    = 0x0040;
        public static final int O_EXCL     = 0x0080;
        public static final int O_NOCTTY   = 0x0100;
        public static final int O_TRUNC    = 0x0200;
        public static final int O_APPEND   = 0x0400;
        public static final int O_NONBLOCK = 0x0800;

        // Sync / async
        public static final int O_DSYNC  = 0x1000;
        public static final int FASYNC   = 0x2000;
        public static final int O_DIRECT = 0x4000;

        // Large / path semantics
        public static final int O_LARGEFILE = 0x08000;
        public static final int O_DIRECTORY = 0x10000;
        public static final int O_NOFOLLOW  = 0x20000;
        public static final int O_NOATIME   = 0x40000;
        public static final int O_CLOEXEC   = 0x80000;

        // Sync semantics
        private static final int __O_SYNC = 0x100000;
        public static final int O_SYNC    = __O_SYNC | O_DSYNC;

        // Special modes
        public static final int O_PATH       = 0x200000;
        private static final int __O_TMPFILE = 0x400000;
        public static final int O_TMPFILE    = __O_TMPFILE | O_DIRECTORY;

        // Alias
        public static final int O_NDELAY = O_NONBLOCK;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/stat.h#L9
    public static final class FileMode {
        private FileMode() {}

        // File type bits (st_mode & S_IFMT)
        public static final int S_IFMT   = 0xF000;
        public static final int S_IFSOCK = 0xC000;
        public static final int S_IFLNK  = 0xA000;
        public static final int S_IFREG  = 0x8000;
        public static final int S_IFBLK  = 0x6000;
        public static final int S_IFDIR  = 0x4000;
        public static final int S_IFCHR  = 0x2000;
        public static final int S_IFIFO  = 0x1000;

        // User permissions
        public static final int S_IRUSR = 0x100;
        public static final int S_IWUSR = 0x080;
        public static final int S_IXUSR = 0x040;
        public static final int S_IRWXU = 0x1C0;

        // Group permissions
        public static final int S_IRGRP = 0x020;
        public static final int S_IWGRP = 0x010;
        public static final int S_IXGRP = 0x008;
        public static final int S_IRWXG = 0x038;

        // Other permissions
        public static final int S_IROTH = 0x004;
        public static final int S_IWOTH = 0x002;
        public static final int S_IXOTH = 0x001;
        public static final int S_IRWXO = 0x007;

        // Common defaults (permissions only)
        public static final int DEFAULT_FILE_PERMS = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH; // 0644
        public static final int DEFAULT_DIR_PERMS  = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH; // 0755
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/fs.h#L411
    public static final class RwFlags {
        private RwFlags() {}

        public static final int RWF_HIPRI     = 0x00000001;
        public static final int RWF_DSYNC     = 0x00000002;
        public static final int RWF_SYNC      = 0x00000004;
        public static final int RWF_NOWAIT    = 0x00000008;
        public static final int RWF_APPEND    = 0x00000010;
        public static final int RWF_NOAPPEND  = 0x00000020;
        public static final int RWF_ATOMIC    = 0x00000040;
        public static final int RWF_DONTCACHE = 0x00000080;
        public static final int RWF_NOSIGNAL  = 0x00000100;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/io_uring.h#L327
    public static final class FsyncFlags {
        private FsyncFlags() {}

        public static final int IORING_FSYNC_DATASYNC = 1;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/io_uring.h#L376
    public static final class AsyncCancelFlags {
        private AsyncCancelFlags() {}

        public static final int IORING_ASYNC_CANCEL_ALL      = 1 << 0;
        public static final int IORING_ASYNC_CANCEL_FD       = 1 << 1;
        public static final int IORING_ASYNC_CANCEL_ANY      = 1 << 2;
        public static final int IORING_ASYNC_CANCEL_FD_FIXED = 1 << 3;
        public static final int IORING_ASYNC_CANCEL_USERDATA = 1 << 4;
        public static final int IORING_ASYNC_CANCEL_OP       = 1 << 5;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/fcntl.h#L106
    public static final class AtFlags {
        private AtFlags() {}

        public static final int AT_FDCWD            =   -100;
        public static final int AT_SYMLINK_NOFOLLOW = 0x0100;
        public static final int AT_SYMLINK_FOLLOW   = 0x0400;
        public static final int AT_NO_AUTOMOUNT     = 0x0800;
        public static final int AT_EMPTY_PATH       = 0x1000;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/falloc.h#L5
    public static final class FallocateFlags {
        private FallocateFlags() {}

        public static final int FALLOC_FL_ALLOCATE_RANGE = 0x00;
        public static final int FALLOC_FL_KEEP_SIZE      = 0x01;
        public static final int FALLOC_FL_PUNCH_HOLE     = 0x02;
        public static final int FALLOC_FL_NO_HIDE_STALE  = 0x04;
        public static final int FALLOC_FL_COLLAPSE_RANGE = 0x08;
        public static final int FALLOC_FL_ZERO_RANGE     = 0x10;
        public static final int FALLOC_FL_INSERT_RANGE   = 0x20;
        public static final int FALLOC_FL_UNSHARE_RANGE  = 0x40;
        public static final int FALLOC_FL_WRITE_ZEROES   = 0x80;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/stat.h#L203
    public static final class StatxMask {
        private StatxMask() {}

        public static final int STATX_TYPE        = 0x00000001;
        public static final int STATX_MODE        = 0x00000002;
        public static final int STATX_NLINK       = 0x00000004;
        public static final int STATX_UID         = 0x00000008;
        public static final int STATX_GID         = 0x00000010;
        public static final int STATX_ATIME       = 0x00000020;
        public static final int STATX_MTIME       = 0x00000040;
        public static final int STATX_CTIME       = 0x00000080;
        public static final int STATX_INO         = 0x00000100;
        public static final int STATX_SIZE        = 0x00000200;
        public static final int STATX_BLOCKS      = 0x00000400;
        public static final int STATX_BASIC_STATS = 0x000007ff;
        public static final int STATX_BTIME       = 0x00000800;
        public static final int STATX_ALL         = STATX_BASIC_STATS | STATX_BTIME;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/fcntl.h#L145
    public static final class StatxFlags {
        private StatxFlags() {}

        public static final int AT_STATX_SYNC_AS_STAT = 0x0000;
        public static final int AT_STATX_FORCE_SYNC   = 0x2000;
        public static final int AT_STATX_DONT_SYNC    = 0x4000;
        public static final int AT_STATX_SYNC_TYPE    = 0x6000;
        public static final int AT_RECURSIVE          = 0x8000;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/linux/splice.h#L17
    public static final class SpliceFlags {
        private SpliceFlags() {}

        public static final int SPLICE_F_MOVE	  = 0x01;
        public static final int SPLICE_F_NONBLOCK = 0x02;
        public static final int SPLICE_F_MORE	  = 0x04;
        public static final int SPLICE_F_GIFT	  = 0x08;
        public static final int SPLICE_F_ALL      = SPLICE_F_MOVE | SPLICE_F_NONBLOCK | SPLICE_F_MORE | SPLICE_F_GIFT;

        // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/io_uring.h#L345
        public static final int SPLICE_F_FD_IN_FIXED = 1 << 31;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/fcntl.h#L180
    public static final class UnlinkAtFlags {
        private UnlinkAtFlags() {}

        public static final int AT_REMOVEDIR = 0x0200;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/fadvise.h#L5
    public static final class FadviseAdvice {
        private FadviseAdvice() {}

        public static final int POSIX_FADV_NORMAL     = 0;
        public static final int POSIX_FADV_RANDOM     = 1;
        public static final int POSIX_FADV_SEQUENTIAL = 2;
        public static final int POSIX_FADV_WILLNEED   = 3;

        // Ignoring s390-64
        public static final int POSIX_FADV_DONTNEED   = 4;
        public static final int POSIX_FADV_NOREUSE    = 5;
    }

    // https://github.com/torvalds/linux/blob/f8f9c1f4d0c7a64600e2ca312dec824a0bc2f1da/include/uapi/linux/fs.h#L397
    public static final class SyncFileRangeFlags {
        private SyncFileRangeFlags() {}

        public static final int SYNC_FILE_RANGE_WAIT_BEFORE    = 1 << 0;
        public static final int SYNC_FILE_RANGE_WRITE          = 1 << 1;
        public static final int SYNC_FILE_RANGE_WAIT_AFTER     = 1 << 2;
        public static final int SYNC_FILE_RANGE_WRITE_AND_WAIT = SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;
    }
}