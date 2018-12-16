## 1. 赛题背景

PolarDB作为软硬件结合的代表, 充分使用新硬件, 榨干硬件的红利来为用户获取极致的数据性能, 其中在PolarDB 的设计中, 我们使用 Optane SSD作为所有热数据的写入缓冲区, 通过kernel bypass的方式, 实现了极致的性能。所以本次比赛就以Optane SSD盘为背景，参赛者在其基础之上探索实现一种高效的kv存储引擎

## 2. 初赛赛题描述

### 2.1 题目内容

实现一个简化、高效的kv存储引擎，支持Write、Read接口

### 2.2 语言限定

C++ & JAVA

**注：C++和JAVA一起排名**

### 2.3 程序目标
项目地址：[engine](https://code.aliyun.com/polar_race2018/engine?spm=a2111a.8458726.0.0.6c6c7a7ftf0MdV)

**C++:** 请仔细阅读engine/include/engine.h代码了解基类Engine定义，我们已为大家定义了比赛专用的子类EngineRace（在engine/engine_race目录下），参赛者通过编写代码，完善EngineRace来实现自己的kv存储引擎

**Java：**请仔细阅读engine/engine_java/src/main/java/com/alibabacloud/polar_race/engine/common/AbstractEngine.java了解基类AbstractEngine定义，我们已为大家定义了比赛专用的子类EngineRace（在engine/engine_java/src/main/java/com/alibabacloud/polar_race/engine/common/EngineRace.java文件中），参赛者通过编写代码，完善EngineRace来实现自己的kv存储引擎

**注：**

**1. 请完善我们已经定义好的子类EngineRace即可，不需要额外继承基类，否则评测程序可能会找不到实现代码**

**2. 调试日志信息请使用标准输出，评测程序会将标准输出重定向到日志文件并放到OSS上供参赛者查看，仅保留最近一次运行日志，最大100M（超过100M则无法提供本次运行日志），日志下载方法为：wget http://polardbrace.oss-cn-shanghai.aliyuncs.com/log_TEAMID.tar (替换TEAMID为自己ID即可)**

### 2.4 参赛方法说明

1. 在阿里天池找到"POLARDB 数据库性能大赛"，并报名参加
2. 在code.aliyun.com注册一个账号，fork本仓库的代码（**将polar_race2018添加为Reporter，否则测试程序没有权限拉取代码**），重写EngineRace类中相关函数的实现
3. 在天池提交成绩的入口，提交自己fork的仓库git地址**（提交时在“镜像路径”一栏填写自己的开发语言，CPP或者JAVA，大小写不限）**
4. 等待评测结果

**注：首次提交代码时，首先在天池页面点击“提交结果”->“修改地址”，在弹出的窗口中“git路径”一栏不需要填写“git@code.aliyun.com:”，只需填写自己仓库名即可，即：“USERNAME/engine.git”**

### 2.5 测试环境

CPU：Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz （64核）

磁盘：Intel Corporation Device 2701（345G、裸盘IOPS 580K、EXT4 IOPS 400K）

OS：Linux version 3.10.0-327.ali2010.alios7.x86_64

文件系统：EXT4

ulimit -a：

```
-t: cpu time (seconds)              unlimited
-f: file size (blocks)              unlimited
-d: data seg size (kbytes)          unlimited
-s: stack size (kbytes)             8192
-c: core file size (blocks)         0
-m: resident set size (kbytes)      unlimited
-u: processes                       948671
-n: file descriptors                655350
-l: locked-in-memory size (kbytes)  64
-v: address space (kbytes)          unlimited
-x: file locks                      unlimited
-i: pending signals                 948671
-q: bytes in POSIX msg queues       819200
-e: max nice                        0
-r: max rt priority                 0
-N 15:                              unlimited
```

JDK：

```
java version "1.8.0_152"
OpenJDK Runtime Environment (Alibaba 8.4.8) (build 1.8.0_152-b211)
OpenJDK 64-Bit Server VM (Alibaba 8.4.8) (build 25.152-b211, mixed mode)
```

Java JVM参数（已针对比赛场景进行了调优）:

```
-server
-Xms2560m
-Xmx2560m
-XX:MaxDirectMemorySize=256m
-XX:NewRatio=1
-XX:+UseConcMarkSweepGC
-XX:+UseParNewGC
-XX:-UseBiasedLocking
```

### 2.6 程序评测逻辑

评测程序分为2个阶段：

1. **Recover正确性评测**

   此阶段评测程序会并发写入特定数据（key 8B、value 4KB）同时进行任意次kill -9来模拟进程意外退出（参赛引擎需要保证进程意外退出时数据持久化不丢失），接着重新打开DB，调用Read接口来进行正确性校验

2. **性能评测**

   2.1 随机写入：64个线程并发随机写入，每个线程使用Write各写100万次随机数据（key 8B、value 4KB）

   2.2 随机读取：64个线程并发随机读取，每个线程各使用Read读取100万次随机数据

   2.3 顺序读取：64个线程并发顺序读取，每个线程使用Range全局**顺序**迭代DB数据2次

**注：**

**1. 共2个维度测试性能，每一个维度测试结束后会保留DB数据，关闭Engine实例，重启进程，清空PageCache，下一个维度开始后重新打开新的Engine实例**

**2. 读取阶段会数据进行校验，没有通过的话则评测不通过**

**3.参赛引擎只需保证进程意外退出时数据持久化不丢失即可，不要求保证在系统crash时的数据持久化不丢失**

**4. 整个评测会有时间限制（具体时限待公布），超时后评测结束且对应提交无结果**

### 2.7 排名规则

在Recover正确性验证通过的情况下，对性能评测阶段整体计时，如果该阶段的正确检测全部通过，则成绩有效，根据总用时从低到高进行排名（用时越短排名越靠前）

**注：每次评测结束后，评测程序会将性能评测的结果输出到文件，和用户调试日志文件一起打包上传到OSS供参赛者查看，仅保留最近一次运行结果信息，日志下载方法同用户调试日志下载方法

### 2.8 资源限制

内存占用不得超过：2G（C++), 3G(JAVA)

磁盘占用不得超过：320G

不使用压缩库

### 2.9 作弊说明

1. 不得使用第三方kv引擎库（如RocksDB、LevelDB、LMDB等），在其之上封装接口用来参赛

2. 禁止使用tmpfs以及share memory等内存。包括但不限于： /dev/shm、/dev/pts、/sys /proc

3. 如果发现有作弊行为，包括但不限于：
    （1）通过hack评测程序，绕过了必须的评测逻辑
    （2）通过猜测数据格式进行针对性的压缩
    （3）窃取评测程序代码
     ... ...
    则成绩无效，且取消参赛资格。

## 3 复赛赛题描述

复赛赛题及要求除如下增项外，其他同初赛一样：

### 3.1 题目内容

在初赛题目基础上，还需要额外实现一个Range接口

### 3.2 程序评测逻辑

评测程序分为2个阶段：

1. **Recover正确性评测**

此阶段评测程序会并发写入特定数据（key 8B、value 4KB）同时进行任意次kill -9来模拟进程意外退出（参赛引擎需要保证进程意外退出时数据持久化不丢失），接着重新打开DB，调用Read、Range接口来进行正确性校验

2. **性能评测**

2.1 随机写入：64个线程并发随机写入，每个线程使用Write各写100万次随机数据（key 8B、value 4KB）

2.2 随机读取：64个线程并发随机读取，每个线程各使用Read读取100万次随机数据

2.3 顺序读取：64个线程并发顺序读取，每个线程各使用Range**有序（增序）**遍历全量数据**2**次

**注：顺序读取阶段除了对迭代出来每条的kv校验是否匹配外，还会额外校验是否严格递增，如不通过则终止，评测失败**