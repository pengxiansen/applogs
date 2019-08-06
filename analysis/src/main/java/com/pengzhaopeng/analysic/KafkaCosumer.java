package com.pengzhaopeng.analysic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

/**
 * 将Kafka数据导入到HDFS
 * 消费三种不同的日志 导入到HDFS不同的目录
 * 启动，跳转，错误日志
 */
public class KafkaCosumer {

    private static String hdfsBasicPath = "hdfs://192.168.2.4:9000/user/applogs/";

    private static FileSystem fs = null;
    private static Configuration configuration = new Configuration();

    private static FSDataOutputStream startUpOs = null;
    private static FSDataOutputStream visitOs = null;
    private static FSDataOutputStream errorOs = null;

    private static Path startUpWritePath = null;
    private static Path visitWritePath = null;
    private static Path errorWritePath = null;

    public static void main(String[] args) {
        //创建配置
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "hadoop01:9092");
        // 制定consumer group
        props.put("group.id", "group2");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe(Collections.singletonList("topic_app_startup"));

        //获取HDFS文件系统
        try {
            fs = FileSystem.get(new URI("hdfs://192.168.2.4:9000"), configuration, "root");
        } catch (Exception e) {
            e.printStackTrace();
        }

        //获取当前时间
        long lastTime = System.currentTimeMillis();

        //获取数据写入全路径 （hdsfPath/typelog/yyyyMM/dd/HHmm）
        String startUpPath = getTotalPath(0, lastTime);
        String visitPath = getTotalPath(1, lastTime);
        String errorPath = getTotalPath(2, lastTime);

        //根据路径创建Path对象
        startUpWritePath = new Path(startUpPath);
        visitWritePath = new Path(visitPath);
        errorWritePath = new Path(errorPath);

        //创建文件流
        try {
            //启动
            if (fs.exists(startUpWritePath)) {
                startUpOs = fs.append(startUpWritePath);
            } else {
                startUpOs = fs.create(startUpWritePath, true);
            }

            //访问
            if (fs.exists(visitWritePath)) {
                visitOs = fs.append(visitWritePath);
            } else {
                visitOs = fs.create(visitWritePath, true);
            }

            //错误
            if (fs.exists(errorWritePath)) {
                errorOs = fs.append(errorWritePath);
            } else {
                errorOs = fs.create(errorWritePath, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                String value = record.value();
                if (value != null && !value.isEmpty()) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    Integer type = jsonObject.getInteger("type");

                    //收集一分钟的数据后更换目录
                    if (System.currentTimeMillis() - lastTime > 1000 * 60) {
                        lastTime = changePath(lastTime, type);
                    }

                    //目录搞定后就将日志文件写入到HDFS中
                    String log = value + "\r\n";
                    try {
                        switch (type){
                            case 0:
                                startUpOs.write(log.getBytes());
                                //一致性模型
                                startUpOs.hflush();
                                startUpOs.hsync();
                                break;
                            case 1:
                                visitOs.write(log.getBytes());
                                //一致性模型
                                visitOs.hflush();
                                visitOs.hsync();
                                break;
                            case 2:
                                errorOs.write(log.getBytes());
                                //一致性模型
                                errorOs.hflush();
                                errorOs.hsync();
                                break;
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

    }

    /**
     * 重新获取时间，更新目录
     *
     * @param lastTime
     * @param type
     * @return
     */
    private static long changePath(long lastTime, Integer type) {
        try {
            //重新获取时间，更新目录
            Long currentTime = System.currentTimeMillis();
            String newPath = getTotalPath(type, currentTime);
            switch (type) {
                case 0: //启动
                    if (startUpOs != null) {
                        startUpOs.close();
                    }

                    startUpWritePath = new Path(newPath);
                    startUpOs = fs.create(startUpWritePath);
                    break;
                case 1: //访问
                    if (visitOs != null) {
                        visitOs.close();
                    }

                    visitWritePath = new Path(newPath);
                    visitOs = fs.create(visitWritePath);
                    break;
                case 2: //错误
                    if (errorOs != null) {
                        errorOs.close();
                    }

                    errorWritePath = new Path(newPath);
                    errorOs = fs.create(errorWritePath);
                    break;
            }

            lastTime = currentTime;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return lastTime;
    }

    /**
     * 根据时间和业务类型获取HDFS路径名称
     */
    private static String getTotalPath(int type, Long lastTime) {
        //时间格式转换 （yyyyMM-dd-HHmm）
        String formatDate = timeTransfrom(lastTime);
        //提取目录（yyyyMM/dd/）
        String directory = getDirectoryFromDate(formatDate);
        //提取文件名称 (HHmm)
        String fileName = getFileName(formatDate);
        //全路径
        String totalPath = null;
        switch (type) {
            case 0: //启动
                totalPath = hdfsBasicPath + "start_up_log" + "/" + directory + "/" + fileName;
                break;
            case 1: //跳转
                totalPath = hdfsBasicPath + "page_visit_log" + "/" + directory + "/" + fileName;
                break;
            case 2: //错误
                totalPath = hdfsBasicPath + "error_log" + "/" + directory + "/" + fileName;
                break;
        }
        return totalPath;
    }

    private static String timeTransfrom(Long timeInMills) {
        Date time = new Date(timeInMills);
        String formatDate = "";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM-dd-HHmm");
            formatDate = sdf.format(time);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return formatDate;
    }

    private static String getDirectoryFromDate(String date) {
        // yyyyMM-dd-HHmm
        String[] split = date.split("-");
        //yyyyMM/dd/
        return split[0] + "/" + split[1];
    }

    private static String getFileName(String date) {
        //yyyyMM-dd-HHmm  HHmm
        String[] split = date.split("-");
        return split[2];
    }
}
