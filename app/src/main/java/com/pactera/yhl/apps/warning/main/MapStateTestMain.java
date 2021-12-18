package com.pactera.yhl.apps.warning.main;

/**
 * @author SUN KI
 * @time 2021/11/22 18:30
 * @Desc
 */
public class MapStateTestMain {
    public static void main(String[] args) throws  Exception{
//        //获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置并行度
//        env.setParallelism(2);
//        //获取数据源
//        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
//                env.fromElements(
//                        Tuple2.of(1L, 3L),
//                        Tuple2.of(1L, 7L),
//                        Tuple2.of(2L, 4L),
//                        Tuple2.of(1L, 5L),
//                        Tuple2.of(2L, 2L),
//                        Tuple2.of(2L, 6L));
//
//
//        // 输出：
//        //(1,5.0)
//        //(2,4.0)
//        dataStreamSource
//                .keyBy(x -> x.f0)
//                .flatMap(new CountAverageWithMapState())
//                .print();
//
//
//        env.execute("TestStatefulApi");
//    }
//    public static class CountAverageWithMapState
//            extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
//
//        private MapState<String,Long> mapState;
//
//        /***状态初始化*/
//        @Override
//        public void open(Configuration parameters) throws Exception {
//
//            MapStateDescriptor descriptor = new MapStateDescriptor("MapDescriptor",String.class,String.class);
//            mapState = getRuntimeContext().getMapState(descriptor);
//
//        }
//
//        @Override
//        public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> collector) throws Exception {
//
//            //获取状态
//            mapState.put(UUID.randomUUID().toString(),element.f1);
//            List<Long> allEles = Lists.newArrayList(mapState.values());
//
//            if(allEles.size() >=3){
//                long count = 0;
//                long sum = 0;
//                for (Long ele:allEles) {
//                    count ++;
//                    sum += ele;
//                }
//                double avg = (double) sum/count;
//                collector.collect(Tuple2.of(element.f0,avg));
//                mapState.clear();
//            }
//        }
//        Long a = 1L;
//        Long b = 2L;
//        double c = a*1.0 / b;
//        System.out.println(String.valueOf(c));
        String string = "2021-11-11 00:01:02";
        System.out.println(string.substring(0, 10));

    }
}
