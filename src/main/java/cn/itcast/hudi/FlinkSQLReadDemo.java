package cn.itcast.hudi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 基于Flink SQL Connector实现：从Hudi表中加载数据，编写SQL查询
 */
public class FlinkSQLReadDemo {

	public static void main(String[] args) {
		// 1-获取表执行环境
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.inStreamingMode()
			.build();
		TableEnvironment tableEnv = TableEnvironment.create(settings) ;

		// 2-创建输入表，TODO：加载Hudi表数据
		tableEnv.executeSql(
			"CREATE TABLE order_hudi(\n" +
				"  orderId STRING PRIMARY KEY NOT ENFORCED,\n" +
				"  userId STRING,\n" +
				"  orderTime STRING,\n" +
				"  ip STRING,\n" +
				"  orderMoney DOUBLE,\n" +
				"  orderStatus INT,\n" +
				"  ts STRING,\n" +
				"  partition_day STRING\n" +
				")\n" +
				"PARTITIONED BY (partition_day)\n" +
				"WITH (\n" +
				"    'connector' = 'hudi',\n" +
				"    'path' = 'file:///D:/flink_hudi_order',\n" +
				"    'table.type' = 'MERGE_ON_READ',\n" +
				"    'read.streaming.enabled' = 'true',\n" +
				"    'read.streaming.check-interval' = '4'\n" +
				")"
		);

		// 3-执行查询语句，读取流式读取Hudi表数据
		tableEnv.executeSql(
			"SELECT orderId, userId, orderTime, ip, orderMoney, orderStatus, ts, partition_day FROM order_hudi"
		).print() ;
	}

}
