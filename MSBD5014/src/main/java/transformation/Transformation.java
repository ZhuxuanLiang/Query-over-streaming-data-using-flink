package transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.sink.FileSink;
import datasource.MainSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import table.*;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Transformation {
    public static final OutputTag<Customer> CustTag = new OutputTag<Customer>("CustomerStream"){};
    public static OutputTag<Lineitem> LineTag = new OutputTag<Lineitem>("LineitemStream"){};
    public static OutputTag<Nation> NationTag = new OutputTag<Nation>("NationStream"){};
    public static OutputTag<Orders> OrderTag = new OutputTag<Orders>("OrderStream"){};
    public static OutputTag<Supplier> SupplierTag = new OutputTag<Supplier>("SupplierStream"){};
    public static OutputTag<Region> RegionTag = new OutputTag<Region>("RegionStream"){};
    public static OutputTag<Part> PartTag = new OutputTag<Part>("PartStream"){};
    public static void main(String[] args) throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String filePath="/Users/phoebe/Documents/BDT/ip/myProject/mergefile/MergeFile.tbl";
        MainSource mainsource = new MainSource(filePath);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        checkpoint(env);
        long startTime=System.currentTimeMillis();
        DataStreamSource<String> mainstream = env.addSource(mainsource).setParallelism(1);
        env.setParallelism(4);
        SingleOutputStreamOperator<String> processedstream = mainstream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                String[] str = s.split("\\|");
                if(str[2].equals("customer")){
                    Customer cust = new Customer(Long.valueOf(str[3]),
                            str[4],
                            str[5],
                            Long.valueOf(str[6]),
                            str[7],
                            Double.valueOf(str[8]),
                            str[9],
                            str[10],
                            str[1]
                    );
                    context.output(CustTag, cust);
               }else if(str[2].equals("lineitem")){
                    Lineitem lineitem = new Lineitem(Long.valueOf(str[3]),
                            Long.valueOf(str[4]),
                            Long.valueOf(str[5]),
                            Integer.valueOf(str[6]),
                            Double.valueOf(str[7]),
                            Double.valueOf(str[8]),
                            Double.valueOf(str[9]),
                            Double.valueOf(str[10]),
                            str[11],
                            str[12],
                            sdf.parse(str[13]),
                            sdf.parse(str[14]),
                            sdf.parse(str[15]),
                            str[16],
                            str[17],
                            str[18],
                            str[1]
                    );
                    context.output(LineTag, lineitem);
                }else if(str[2].equals("nation")){
                    Nation nation = new Nation(Long.valueOf(str[3]),
                            str[4],
                            Long.valueOf(str[5]),
                            str[6],
                            str[1]
                    );
                    context.output(NationTag, nation);
                }else if(str[2].equals("orders")){
                    Orders order = new Orders(Long.valueOf(str[3]),
                            Long.valueOf(str[4]),
                            str[5],
                            Double.valueOf(str[6]),
                            sdf.parse(str[7]),
                            str[8],
                            str[9],
                            Integer.valueOf(str[10]),
                            str[11],
                            str[1]
                    );
                    context.output(OrderTag, order);
                }else if(str[2].equals("supplier")){
                    Supplier supplier = new Supplier(Long.valueOf(str[3]),
                            str[4],
                            str[5],
                            Long.valueOf(str[6]),
                            str[7],
                            Double.valueOf(str[8]),
                            str[9],
                            str[1]
                    );
                    context.output(SupplierTag,supplier);
                }else if(str[2].equals("region")){
                    Region region = new Region(Long.valueOf(str[3]),
                            str[4],
                            str[1]
                    );
                    context.output(RegionTag, region);
                }else if(str[2].equals("part")){
                    Part part = new Part(Long.valueOf(str[3]),
                            str[4],
                            str[5],
                            str[6],
                            str[7],
                            Integer.valueOf(str[8]),
                            str[9],
                            Double.valueOf(str[10]),
                            str[11],
                            str[1]
                    );
                    context.output(PartTag, part);
                }

                if(str[0].equals("1")){
                    Thread.sleep(3000);
                    System.out.println("===============25%===============");
                }else if(str[0].equals("2")){
                    Thread.sleep(3000);
                    System.out.println("===============50%===============");
                }else if(str[0].equals("3")){
                    Thread.sleep(3000);
                    System.out.println("===============75%===============");
                }else if(str[0].equals("4")){
                    Thread.sleep(3000);
                    System.out.println("===============100%===============");
                }

            }
        }).setParallelism(1);
        DataStream<Customer> custStream = processedstream.getSideOutput(CustTag);
        DataStream<Lineitem> lineStream = processedstream.getSideOutput(LineTag);
        DataStream<Nation> nationStream = processedstream.getSideOutput(NationTag);
        DataStream<Orders> orderStream = processedstream.getSideOutput(OrderTag);
        DataStream<Supplier> supplierStream = processedstream.getSideOutput(SupplierTag);
        DataStream<Region> regionStream = processedstream.getSideOutput(RegionTag);
        DataStream<Part> partStream = processedstream.getSideOutput(PartTag);

//       processedstream.getSideOutput(SupplierTag).print("supplier");
        String date1 = "1995-01-01";
        String date2 = "1996-12-31";
        Date dt1 = sdf.parse(date1);
        Date dt2 = sdf.parse(date2);
        String REGION = "AMERICA";
        String TYPE = "ECONOMY ANODIZED STEEL";
        String NATION = "FRANCE";
        String outputPath = "output";
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartSuffix(".csv")
                .build();

        final FileSink<String> sink = FileSink.<String>forRowFormat(new Path(outputPath), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024*1024*1024)
                                .build())
                .withOutputFileConfig(config)
                .build();

        SingleOutputStreamOperator<Tuple5<String, Integer, Double, Long,String>> process1=

                regionStream.filter(value -> value.r_name.equals(REGION))
                        .connect(nationStream)
                        .keyBy(value -> value.r_regionkey, value->value.n_regionkey)
                        .process(new RegionNationProcessFunction())
                .connect(custStream)
                .keyBy(value -> value.f0, value -> value.c_nationkey)
                .process(new NationCustProcessFunction())
                .connect(orderStream.filter(value -> !value.o_orderdate.before(dt1) && !value.o_orderdate.after(dt2)))
                .keyBy(value -> value.f0, value -> value.o_custkey)
                .process(new CustOrderProcessFunction())
                .map(new ExtractYear())
                .connect(lineStream)
                .keyBy(value -> value.f0, value -> value.l_orderkey)
                .process(new OrderLineProcessFunction());

        SingleOutputStreamOperator<Tuple3<String, String,String>> process2 =
                nationStream.connect(supplierStream)
                       .keyBy(value->value.n_nationkey,value-> value.s_nationkey)
                .process(new NationSuppProcessFunction())
                .connect(lineStream)
                .keyBy(value -> value.f1, value -> value.l_suppkey)
                .process(new SuppLineProcessFunction());

        process1.connect(partStream.filter(value->value.p_type.equals(TYPE)))
                .keyBy(value->value.f3, value-> value.p_partkey)
                .process(new ConnectPartProcessFunction())
                .connect(process2)
                .keyBy(value -> value.f0, value-> value.f1)
                .process(new ConnectTwoStream())
                .keyBy(value->value.f0)
                        .process(new AggProcessFunction()).print();
        //System.out.println(env.getExecutionPlan());
        env.execute();
        long endTime=System.currentTimeMillis();
        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
    }
    private static void checkpoint(StreamExecutionEnvironment env){
        env.enableCheckpointing(1000);//一般秒级、分钟级
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/phoebe/Documents/flink/checkpoints");
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);//这里设置10分钟
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));
    }
}


