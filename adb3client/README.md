# 通过adb3client读写ADB

## 功能介绍
adbclient的主要目的时简化读写ADB(3.0)的操作，避免直接使用JDBC方式需要处理连接池、线程池、重试机制等一系列问题。adb3client基于JDBC实现，当前版本提供了批量插入数据和执行自定义操作的功能。

## 引入依赖
- Maven
```xml
<dependency>
  <groupId>com.alibaba.cloud.analyticdb</groupId>
  <artifactId>adb3client</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## 数据写入
建议项目中创建AdbClient单例，通过writeThreadSize和readThreadSize控制读写的并发（每并发占用1个JDBC连接，空闲超过connectionMaxIdleMs将被自动回收)
```java
// 配置参数,url格式为 jdbc:postgresql://host:port/db
AdbConfig config = new AdbConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);

try (AdbClient client = new AdbClient(config)) {
    TableSchema schema0 = client.getTableSchema("t0");
				
    Put put = new Put(schema1);
    put.setObject(0, 1);
    put.setObject(1, 2);
    client.put(put);
	...
    //强制提交所有未提交put请求；AdbClient内部也会根据WriteBatchSize、WriteBatchByteSize、writeMaxIntervalMs三个参数自动提交
    client.flush();

} catch (AdbClientException e) {
    e.printStackTrace();
}
```

## 异常处理
```java
public void doPut(AdbClient client, Put put) throws AdbClientException {
    try {
        client.put(put);
    } catch (AdbClientWithDetailsException e) {
        for(int i=0;i<e.size();++i){
            //写入失败的记录
            Record failedRecord = e.getFailRecord(i);
            //写入失败的原因
            AdbClientException cause = e.getException(i);
            //脏数据处理逻辑
        }
    } catch (AdbClientException e) {
        //非AdbClientWithDetailsException的异常一般是fatal的
        throw e;
    }
}

public void doFlush(AdbClient client) throws AdbClientException {
    try {
        client.flush();
    } catch (AdbClientWithDetailsException e) {
        for(int i=0;i<e.size();++i){
            //写入失败的记录
            Record failedRecord = e.getFailRecord(i);
            //写入失败的原因
            AdbClientException cause = e.getException(i);
            //脏数据处理逻辑
        }
    } catch (AdbClientException e) {
        //非AdbClientWithDetailsException的异常一般是fatal的
        throw e;
    }
}

```

## 自定义操作
```java
AdbConfig config = new AdbConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);
try (AdbClient client = new AdbClient(config)) {
    client.sql(conn -> {
				try (Statement stat = conn.createStatement()) {
					stat.execute("create table t0(id int)");
				}
				return null;
			}).get();
} catch (AdbClientException e) {
}
```
## 实现
### 线程模型和连接池模型
AdbClient会启动若干worker线程，查询和写入都会在worker中执行。在Fe模式下，读写共用wroker；FixedFe模式模式下，put操作单独使用一个线程池，sql、getTableSchema接口使用另一个线程池。

连接池使用Druid管理，最大连接数为线程池大小的2倍。连接在worker之间复用。连接池的配置如下：
```java
		dataSource.setMaxWait(60000);
		dataSource.setMinEvictableIdleTimeMillis(600000);
		dataSource.setMaxEvictableIdleTimeMillis(900000);
		dataSource.setTimeBetweenEvictionRunsMillis(2000);
		dataSource.setTestWhileIdle(true);
		dataSource.setTestOnBorrow(false);
		dataSource.setTestOnReturn(false);
		dataSource.setKeepAlive(true);
		dataSource.setKeepAliveBetweenTimeMillis(30000);
		dataSource.setPhyMaxUseCount(1000);
		dataSource.setValidationQuery("select 1");
```

### 写入
写入将一批Put放入多个队列后，在内存中合并成Batch提交给Worker，worker对batch做批量提交。批量提交通过在jdbcurl后面附加rewriteBatchedStatements=true属性来实现。

## 附录
### AdbConfig参数说明
#### 基础配置
| 参数名 | 默认值 | 说明 |引入版本|
| --- | -- | --- | --- |
| jdbcUrl | 无 | 必填| 1.0.0 |
| username | 无 | 必填 | 1.0.0 |
| password | 无 | 必填 | 1.0.0 |
| appName | adb3client | 线程池名称为embedded-$appName | 1.0.0 |

#### 写入配置
| 参数名                                   | 默认值 | 说明                                                                                                                                                               | 引入版本  | 
|---------------------------------------| --- |------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|
| writeThreadSize                       | 1 | 处理AdbClient.put方法请求的最大连接数                                                                                                                                        | 1.0.0 |
| [writeMode](#writeMode)               | INSERT_OR_REPLACE | 当INSERT目标表为有主键的表时采用不同策略<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_REPLACE当主键冲突时，更新所有列                                                                          | 1.0.0 |
| writeBatchSize                        | 512 | 每个写入线程的最大批次大小，在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交                                                                                                     | 1.0.0 |
| writeBatchByteSize                    | 2097152（2 * 1024 * 1024） | 每个写入线程的最大批次bytes大小，单位为Byte，默认2MB，<br>在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交                                                                       | 1.0.0 |
| writeBatchTotalByteSize               | 20971520（20 * 1024 * 1024） | 所有表最大批次bytes大小，单位为Byte，默认20MB，在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交                                                                              | 1.0.0 |
| writeMaxIntervalMs                    | 10000 | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交                                                                                                                              | 1.0.0 |
| inputNumberAsEpochMsForDatetimeColumn | false | 当Number写入Date/timestamp/timestamptz列时，若为true，将number视作ApochMs                                                                                                    | 1.0.0 |
| inputStringAsEpochMsForDatetimeColumn | false | 当String写入Date/timestamp/timestamptz列时，若为true，将String视作ApochMs                                                                                                    | 1.0.0 |
| removeU0000InTextColumnValue          | true | 当写入Text/Varchar列时，若为true，剔除字符串中的\u0000                                                                                                                           | 1.0.0 |
| enableDefaultForNotNullColumn         | true | 启用时，not null且未在表上设置default的字段传入null时，将以默认值写入. String 默认“”,Number 默认0,Date/timestamp/timestamptz 默认1970-01-01 00:00:00                                            | 1.0.0 |
| defaultTimeStampText                  | null | enableDefaultForNotNullColumn=true时，Date/timestamp/timestamptz的默认值                                                                                               | 1.0.0 |

#### 连接配置
| 参数名 | 默认值 | 说明 |引入版本| 
| --- | --- | --- | --- |
| retryCount | 3 | 当连接故障时，写入和查询的重试次数 | 1.0.0|
| retrySleepInitMs | 1000 | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs | 1.0.0 |
| retrySleepStepMs | 10000 | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |1.0.0 |
| metaCacheTTL | 1 min | getTableSchema信息的本地缓存时间 | 1.0.0 |
| metaAutoRefreshFactor | 4 | 当tableSchema cache剩余存活时间短于 metaCacheTTL/metaAutoRefreshFactor 将自动刷新cache | 1.0.0 |

### 参数详解
#### writeMode
- INSERT_OR_INGORE
```sql
-- 生成如下sql
INSERT IGNORE INTO t0 (pk, c0, c1, c2) values (?, ?, ?, ?);
```
- INSERT_OR_REPLACE
```sql
-- 生成如下sql
REPLACE INTO t0 (pk, c0, c1, c2) values (?, ?, ?);
```