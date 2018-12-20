# 天池大赛-阿里云2018年第一届PolarDB数据库性能大赛分享

[天池比赛链接](https://tianchi.aliyun.com/programming/rankingList.htm?spm=5176.100067.5678.4.5efb714bzvzVTd&raceId=231689)

[赛题描述](RaceRule.md)

[解题报告长文](http://neoremind.com/2018/12/2018_polar_race_java_rank_1_sharing)

TODO 解法报告预期发布在阿里云栖社区，链接会更新。

## How to build

```
cd engine_java && mvn assembly:assembly
```

## How to run

默认只能在Linux上运行（因为使用了JNA的Direct I/O）。

Write:
```
java -server -XX:-UseBiasedLocking -Xms2000m -Xmx2000m -XX:NewSize=1400m -XX:MaxMetaspaceSize=32m -XX:MaxDirectMemorySize=1G -XX:+UseG1GC -cp engine_java-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.alibabacloud.polar_race.engine.common.neoremind.benchmark.BenchMark2 64 1000000 8 4096 /please/set/directory 1
```

Read:
```
java -server -XX:-UseBiasedLocking -Xms2000m -Xmx2000m -XX:NewSize=1400m -XX:MaxMetaspaceSize=32m -XX:MaxDirectMemorySize=1G -XX:+UseG1GC -cp engine_java-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.alibabacloud.polar_race.engine.common.neoremind.benchmark.BenchMark2 64 1000000 8 4096 /please/set/directory 2
```

Range:
```
java -server -XX:-UseBiasedLocking -Xms2000m -Xmx2000m -XX:NewSize=1400m -XX:MaxMetaspaceSize=32m -XX:MaxDirectMemorySize=1G -XX:+UseG1GC -cp engine_java-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.alibabacloud.polar_race.engine.common.neoremind.benchmark.BenchMark2 64 1000000 8 4096 /please/set/directory 2
```

Note:
```
64 1000000 8 4096 表示64并发，每个线程100w kv，key 8B，value 4K
```
