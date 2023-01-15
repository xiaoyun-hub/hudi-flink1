package cn.itcast.hudi;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.* ;

public class FlinkSQLKafkaDemo {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 设置状态后端RocksDB
		EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
		embeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
		env.setStateBackend(embeddedRocksDBStateBackend);

		// checkpoint配置
		env.enableCheckpointing(TimeUnit.SECONDS.toMillis(30), CheckpointingMode.EXACTLY_ONCE);
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointStorage("hdfs://s201:8020/ckps");
		checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(20));
		checkpointConfig.setTolerableCheckpointFailureNumber(5);
		checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(1));
		checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(env);

		// 2-创建输入表，TODO：从Kafka消费数据
		sTableEnv.executeSql(
			"CREATE TABLE order_kafka_source (\n" +
				"  orderId STRING,\n" +
				"  userId STRING,\n" +
				"  orderTime STRING,\n" +
				"  ip STRING,\n" +
				"  orderMoney DOUBLE,\n" +
				"  orderStatus INT\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'order-topic',\n" +
				"  'properties.bootstrap.servers' = 's205:9092',\n" +
				"  'properties.group.id' = 'gid-1001',\n" +
				"  'scan.startup.mode' = 'latest-offset',\n" +
				"  'format' = 'json',\n" +
				"  'json.fail-on-missing-field' = 'false',\n" +
				"  'json.ignore-parse-errors' = 'true'\n" +
				")"
		);

		// 3-转换数据：可以使用SQL，也可以时Table API
		Table etlTable = sTableEnv
			.from("order_kafka_source")
			// 添加字段：Hudi表分区字段， "orderTime": "2021-11-22 10:34:34.136" -> 021-11-22
			.addColumns(
				$("orderTime").substring(0, 10).as("partition_day")
			)
			// 添加字段：Hudi表数据合并字段，时间戳, "orderId": "20211122103434136000001" ->  20211122103434136
			.addColumns(
				$("orderId").substring(0, 17).as("ts")
			);
		sTableEnv.createTemporaryView("view_order", etlTable);

		// 4-创建输入表，TODO: 将结果数据进行输出
		sTableEnv.executeSql("SELECT * FROM view_order").print();
	}

}
