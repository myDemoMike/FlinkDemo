//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
//import org.apache.flink.table.api.Types
//
//val sink = JDBCAppendTableSink.builder
//      .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
//      .setDBUrl(params.getRequired("table_name.output_url"))
//      .setQuery(
//        s"""
//           |INSERT INTO dw_all.${params.getRequired("table_name.output")}
//           |       (asset_id,  date, $getDateUnitField, open, high, low,  close,  pre_close,  volume, turnover, turn_rate,  k, d, j, ema1,  ema2, dea,  sma_u1, sma_d1, rsi1, sma_u2, sma_d2, rsi2, sma_u3, sma_d3, rsi3,
//           |                         open_f, high_f, low_f,  close_f,  pre_close_f                     ,  k_f, d_f, j_f, ema1_f,  ema2_f, dea_f,  sma_u1_f, sma_d1_f, rsi1_f, sma_u2_f, sma_d2_f, rsi2_f, sma_u3_f, sma_d3_f, rsi3_f,
//           |                         open_b, high_b, low_b,  close_b,  pre_close_b                     ,  k_b, d_b, j_b, ema1_b,  ema2_b, dea_b,  sma_u1_b, sma_d1_b, rsi1_b, sma_u2_b, sma_d2_b, rsi2_b, sma_u3_b, sma_d3_b, rsi3_b
//           |       )
//           |VALUES (?      ,   ?,    ?,    ?,    ?,    ?,    ?,      ?,          ?,      ?,        ?,          ?, ?, ?, ?,     ?,    ?,    ?,      ?,      ?,    ?,      ?,      ?,    ?,      ?,      ?,
//           |                         ?,    ?,    ?,    ?,      ?,          ?                           , ?, ?, ?,     ?,    ?,    ?,      ?,      ?,    ?,      ?,      ?,    ?,      ?,      ?,
//           |                         ?,    ?,    ?,    ?,      ?,          ?                           , ?, ?, ?,     ?,    ?,    ?,      ?,      ?,    ?,      ?,      ?,    ?,      ?,      ?
//           |);
//        """.stripMargin)
//      .setParameterTypes(STRING_TYPE_INFO, STRING_TYPE_INFO, INT_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO,
//        DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO,
//        DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO
//      )
//      .setUsername(params.getRequired("clickhouse.username"))
//      .setPassword(params.getRequired("clickhouse.password"))
//      .build
//    tableEnv.registerTableSink(
//      "sink",
//      Array[String]("asset_id",  "date", getDateUnitField(), "open",  "high",  "low",  "close",  "pre_close",  "volume",  "turnover",  "turn_rate",  "k",  "d",  "j",  "ema1",  "ema2",  "dea",  "sma_u1",  "sma_d1",  "rsi1",  "sma_u2",  "sma_d2",  "rsi2",  "sma_u3",  "sma_d3",  "rsi3",
//        "open_f",  "high_f",  "low_f",  "close_f",  "pre_close_f",  "k_f",  "d_f",  "j_f",  "ema1_f",  "ema2_f",  "dea_f",  "sma_u1_f",  "sma_d1_f",  "rsi1_f",  "sma_u2_f",  "sma_d2_f",  "rsi2_f",  "sma_u3_f",  "sma_d3_f",  "rsi3_f",
//        "open_b",  "high_b",  "low_b",  "close_b",  "pre_close_b",  "k_b",  "d_b",  "j_b",  "ema1_b",  "ema2_b",  "dea_b",  "sma_u1_b",  "sma_d1_b",  "rsi1_b",  "sma_u2_b",  "sma_d2_b",  "rsi2_b",  "sma_u3_b",  "sma_d3_b",  "rsi3_b"
//      ),
//      Array[TypeInformation[_]](Types.STRING, Types.STRING, Types.INT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
//        Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
//        Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE
//      ),
//      sink
//    )
//
//    resultTable.insertInto("sink")
