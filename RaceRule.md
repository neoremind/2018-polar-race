## 1. 赛题背景

PolarDB作为软硬件结合的代表, 充分使用新硬件, 榨干硬件的红利来为用户获取极致的数据性能, 其中在PolarDB 的设计中, 我们使用 Optane SSD作为所有热数据的写入缓冲区, 通过kernel bypass的方式, 实现了极致的性能。所以本次比赛就以Optane SSD盘为背景，参赛者在其基础之上探索实现一种高效的kv存储引擎

## 2. 赛题描述

### 2.1 题目内容

实现一个简化、高效的kv存储引擎，支持Write、Read、Range接口

### 2.2 语言限定

C++ & JAVA

**注：C++和JAVA一起排名**

## 3. 程序目标 
项目地址：[engine](https://code.aliyun.com/polar_race2018/engine?spm=a2111a.8458726.0.0.6c6c7a7fY0PJVr)

**C++:** 请仔细阅读engine/include/engine.h代码了解基类Engine定义，我们已为大家定义了比赛专用的子类EngineRace（在engine/engine_race目录下），参赛者通过编写代码，完善EngineRace来实现自己的kv存储引擎

**Java：**请仔细阅读engine/engine_java/src/main/java/com/alibabacloud/polar_race/engine/common/AbstractEngine.java了解基类AbstractEngine定义，我们已为大家定义了比赛专用的子类EngineRace（在engine/engine_java/src/main/java/com/alibabacloud/polar_race/engine/common/EngineRace.java文件中），参赛者通过编写代码，完善EngineRace来实现自己的kv存储引擎

**注：**

**1. 请完善我们已经定义好的子类EngineRace即可，不需要额外继承基类，否则评测程序可能会找不到实现代码**

**2. 调试日志信息请使用标准输出，评测程序会将标准输出重定向到日志文件user_debug_file并放到OSS上供参赛者查看，仅保留最近一次运行日志，最大100M，日志下载路径为：......**

### 4. 参赛方法说明

1. 在阿里天池找到"POLARDB 数据库性能大赛"，并报名参加
2. 在code.aliyun.com注册一个账号，fork本仓库的代码，重写EngineRace类中相关函数的实现
3. 在天池提交成绩的入口，提交自己fork的仓库git地址**（提交时在“镜像路径”一栏填写自己的开发语言，CPP或者JAVA，大小写不限）** 
4. 等待评测结果

**注：首次提交代码时，首先在天池页面点击“提交结果”->“修改地址”，在弹出的窗口中“git路径”一栏不需要填写“git@code.aliyun.com:”，只需填写自己仓库名即可，即：“USERNAME/engine.git”**

### 5. 测试环境

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



### 6. 程序评测逻辑

评测程序分为2个阶段：

1. **正确性评测**

   此阶段评测程序会并发写入特定数据同时进行任意次kill -9来模拟进程意外退出（参赛引擎需要保证进程意外退出时数据持久化不丢失），接着重新打开DB，调用Read、Range接口来进行正确性校验

2. **性能评测**

   2.1 随机写入：32个线程并发随机写入，每个线程使用Write各写100万次随机数据（key 8B、value 4KB）

   2.2 随机读取：32个线程并发随机读取，每个线程各使用Read读取100万次随机数据

   2.3 顺序读取：32个线程并发顺序读取，每个线程使用Range全局顺序迭代DB数据10次

   **注：共3个维度测试性能，每一个维度测试结束后会保留DB数据，关闭Engine实例，清空PageCache，下一个维度开始后重新打开新的Engine实例**

### 7. 排名规则

在正确性验证通过的情况下，对性能评测整体计时，根据总用时从低到高进行排名（用时越短排名越靠前）

**注：每次评测结束后，评测程序会将性能评测的结果输出到benchmark_result_file文件（内包含每个维度测试的详细结果信息，如ops，最大内存使用），和日志文件user_debug_file一起打包上传到OSS供参赛者查看，仅保留最近一次运行结果信息，下载路径为：......**

### 8. 资源限制

*（暂定如下：根据内部赛情况会做调整）*

内存占用不得超过：2G

磁盘占用不得超过：不限制

允许使用的第三方库：snappy、lz4

### 9. 作弊说明

1. 不得使用第三方kv引擎库（如RocksDB、LevelDB、LMDB等），在其之上封装接口用来参赛

2. 禁止使用tmpfs以及share memory等内存。包括但不限于： /dev/shm、/dev/pts、/sys /proc

 如果发现有作弊行为，比如通过hack评测程序，绕过了必须的评测逻辑，则程序无效，且取消参赛资格。
