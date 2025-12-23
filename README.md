## Benchmark

### Hardware

HP SSD EX920 1TB

### Summary

| Test Name | IOPS | Throughput (MB/s) |
| :--- | :--- | :--- |
| **JMH fileChannel_random_read** | 65,166 | 266.92 |
| **JMH ioUring_random_read** | 314,152 | 1,286.77 |
| **JMH fileChannel_random_write** | 215,161 | 881.30 |
| **JMH ioUring_random_write** | 282,187 | 1,155.84 |
| **FIO random_read** | 312,000 | 1,277.00 |
| **FIO random_write** | 285,000 | 1,169.00 |

### fio

#### Read

Random Read, 4K block, 30s test, Direct I/O, Queue Depth 64, 4 threads

```bash

fio \
  --name=random_read_test \
  --ioengine=io_uring \
  --rw=randread \
  --bs=4k \
  --direct=1 \
  --size=1G \
  --numjobs=4 \
  --iodepth=64 \
  --group_reporting \
  --runtime=30 \
  --time_based
```

**Result**

```bash
fio-3.36
Starting 4 processes
random_read_test: Laying out IO file (1 file / 1024MiB)
random_read_test: Laying out IO file (1 file / 1024MiB)
random_read_test: Laying out IO file (1 file / 1024MiB)
random_read_test: Laying out IO file (1 file / 1024MiB)
Jobs: 4 (f=4): [r(4)][100.0%][r=1225MiB/s][r=314k IOPS][eta 00m:00s]
random_read_test: (groupid=0, jobs=4): err= 0: pid=1755532: Wed Dec 24 01:39:25 2025
  read: IOPS=312k, BW=1218MiB/s (1277MB/s)(35.7GiB/30001msec)
    slat (usec): min=2, max=367, avg= 3.44, stdev= 2.23
    clat (usec): min=157, max=2789, avg=817.33, stdev=161.41
     lat (usec): min=164, max=2791, avg=820.77, stdev=161.32
    clat percentiles (usec):
     |  1.00th=[  461],  5.00th=[  562], 10.00th=[  619], 20.00th=[  685],
     | 30.00th=[  734], 40.00th=[  775], 50.00th=[  816], 60.00th=[  848],
     | 70.00th=[  889], 80.00th=[  938], 90.00th=[ 1020], 95.00th=[ 1090],
     | 99.00th=[ 1237], 99.50th=[ 1303], 99.90th=[ 1467], 99.95th=[ 1532],
     | 99.99th=[ 1696]
   bw (  MiB/s): min= 1185, max= 1238, per=100.00%, avg=1218.23, stdev= 2.83, samples=236
   iops        : min=303404, max=317152, avg=311865.98, stdev=724.51, samples=236
  lat (usec)   : 250=0.01%, 500=2.09%, 750=31.22%, 1000=54.75%
  lat (msec)   : 2=11.94%, 4=0.01%
  cpu          : usr=11.96%, sys=26.36%, ctx=1898770, majf=0, minf=288
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued rwts: total=9352404,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: bw=1218MiB/s (1277MB/s), 1218MiB/s-1218MiB/s (1277MB/s-1277MB/s), io=35.7GiB (38.3GB), run=30001-30001msec

Disk stats (read/write):
  nvme0n1: ios=9314383/16, sectors=74516024/208, merge=120/10, ticks=7402026/15, in_queue=7402043, util=67.54%
```

#### Write

Random Write, 4K block, 30s test, Direct I/O, Queue Depth 64, 4 threads

```bash
fio \
  --name=random_write_test \
  --ioengine=io_uring \
  --rw=randwrite \
  --bs=4k \
  --direct=1 \
  --size=1G \
  --numjobs=4 \
  --iodepth=64 \
  --group_reporting \
  --runtime=30 \
  --time_based
```

**Result**

```bash
fio-3.36
Starting 4 processes
random_write_test: Laying out IO file (1 file / 1024MiB)
random_write_test: Laying out IO file (1 file / 1024MiB)
random_write_test: Laying out IO file (1 file / 1024MiB)
random_write_test: Laying out IO file (1 file / 1024MiB)
Jobs: 4 (f=4): [w(4)][100.0%][w=1124MiB/s][w=288k IOPS][eta 00m:00s]
random_write_test: (groupid=0, jobs=4): err= 0: pid=1755555: Wed Dec 24 01:40:07 2025
  write: IOPS=285k, BW=1115MiB/s (1169MB/s)(32.7GiB/30001msec); 0 zone resets
    slat (usec): min=2, max=3875, avg= 4.32, stdev= 5.48
    clat (usec): min=158, max=107156, avg=892.21, stdev=955.07
     lat (usec): min=169, max=107159, avg=896.53, stdev=954.85
    clat percentiles (usec):
     |  1.00th=[  486],  5.00th=[  570], 10.00th=[  603], 20.00th=[  685],
     | 30.00th=[  709], 40.00th=[  750], 50.00th=[  889], 60.00th=[  996],
     | 70.00th=[ 1045], 80.00th=[ 1057], 90.00th=[ 1156], 95.00th=[ 1188],
     | 99.00th=[ 1319], 99.50th=[ 1385], 99.90th=[ 3195], 99.95th=[ 3458],
     | 99.99th=[51119]
   bw (  MiB/s): min=  889, max= 1193, per=100.00%, avg=1115.08, stdev=12.96, samples=236
   iops        : min=227754, max=305542, avg=285461.63, stdev=3316.92, samples=236
  lat (usec)   : 250=0.01%, 500=1.18%, 750=39.29%, 1000=19.80%
  lat (msec)   : 2=39.56%, 4=0.13%, 10=0.01%, 20=0.01%, 50=0.01%
  lat (msec)   : 100=0.01%, 250=0.01%
  cpu          : usr=8.48%, sys=27.50%, ctx=2071500, majf=0, minf=34
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued rwts: total=0,8561439,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
  WRITE: bw=1115MiB/s (1169MB/s), 1115MiB/s-1115MiB/s (1169MB/s-1169MB/s), io=32.7GiB (35.1GB), run=30001-30001msec

Disk stats (read/write):
  nvme0n1: ios=19/8530264, sectors=872/68299656, merge=90/7178, ticks=30/6562986, in_queue=6563121, util=80.26%
```

### JMH

***Note***: *Each op is a batch of 1024 individual writes or reads of 4096 bytes each. Multiplying score by 1024 gives IOPS and multiplying IOPS by 4096 gives bytes / second. Conversely, diving normalized gc rate by 1024 gives bytes per op.*

```
Benchmark                                                         (batchSize)  (bufferSize)   Mode  Cnt       Score      Error   Units
RandomReadBenchmark.fileChannel_random_read                              1024          4096  thrpt    5      63.639 ±    0.339   ops/s
RandomReadBenchmark.fileChannel_random_read:gc.alloc.rate                1024          4096  thrpt    5       0.007 ±    0.054  MB/sec
RandomReadBenchmark.fileChannel_random_read:gc.alloc.rate.norm           1024          4096  thrpt    5     139.465 ± 1064.852    B/op
RandomReadBenchmark.fileChannel_random_read:gc.count                     1024          4096  thrpt    5         ≈ 0             counts
RandomReadBenchmark.ioUring_random_read                                  1024          4096  thrpt    5     306.790 ±    0.913   ops/s
RandomReadBenchmark.ioUring_random_read:gc.alloc.rate                    1024          4096  thrpt    5      35.138 ±   10.411  MB/sec
RandomReadBenchmark.ioUring_random_read:gc.alloc.rate.norm               1024          4096  thrpt    5  124491.193 ±  768.711    B/op
RandomReadBenchmark.ioUring_random_read:gc.count                         1024          4096  thrpt    5       3.000             counts
RandomReadBenchmark.ioUring_random_read:gc.time                          1024          4096  thrpt    5      10.000                 ms
RandomWriteBenchmark.fileChannel_random_write                            1024          4096  thrpt    5     210.119 ±   14.825   ops/s
RandomWriteBenchmark.fileChannel_random_write:gc.alloc.rate              1024          4096  thrpt    5       0.007 ±    0.054  MB/sec
RandomWriteBenchmark.fileChannel_random_write:gc.alloc.rate.norm         1024          4096  thrpt    5      43.127 ±  330.155    B/op
RandomWriteBenchmark.fileChannel_random_write:gc.count                   1024          4096  thrpt    5         ≈ 0             counts
RandomWriteBenchmark.ioUring_random_write                                1024          4096  thrpt    5     275.574 ±   14.921   ops/s
RandomWriteBenchmark.ioUring_random_write:gc.alloc.rate                  1024          4096  thrpt    5      30.570 ±   10.511  MB/sec
RandomWriteBenchmark.ioUring_random_write:gc.alloc.rate.norm             1024          4096  thrpt    5  120537.690 ±  258.222    B/op
RandomWriteBenchmark.ioUring_random_write:gc.count                       1024          4096  thrpt    5       3.000             counts
RandomWriteBenchmark.ioUring_random_write:gc.time                        1024          4096  thrpt    5       9.000                 ms
```
