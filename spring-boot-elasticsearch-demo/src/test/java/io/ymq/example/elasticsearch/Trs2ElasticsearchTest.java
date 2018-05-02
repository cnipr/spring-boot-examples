package io.ymq.example.elasticsearch;


import com.alibaba.fastjson.JSONObject;
import io.ymq.example.elasticsearch.run.Startup;
import io.ymq.example.elasticsearch.trs.TRSOperation;
import io.ymq.example.elasticsearch.utils.ElasticsearchUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

/**
 * 单元测试
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Startup.class)
public class Trs2ElasticsearchTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Trs2ElasticsearchTest.class);

    /**
     * 数据添加
     */
    @Test
    public void addDataTest() {

        for (int i = 0; i < 100; i++) {
            Map<String, Object> map = new HashMap<String, Object>();

            map.put("name", "Trs2ElasticsearchTest" + i);
            map.put("age", i);
            map.put("interests", new String[]{"阅读", "学习"});
            map.put("about", "世界上没有优秀的理念，只有脚踏实地的结果");
            map.put("processTime", new Date());

            ElasticsearchUtils.addData(JSONObject.parseObject(JSONObject.toJSONString(map)), "ymq_index", "about_test", "id=" + i);
        }
    }

    @Test
    public void trs2ElasticsearchTest() throws Exception {
        String[] columns = ("SYSID,申请号,申请日,公开（公告）号,公开（公告）日").split(",");
        String[] columns2 = ("权利要求书,说明书").split(",");
        TRSOperation trsOperation = new TRSOperation();
        List<Map<String, String>> list = trsOperation.queryForList("FMZL", "公开（公告）日>='2017.01.01'",
                columns , columns2,0,16);
        int i = 0;
        for (Map<String, String> map : list) {
            ElasticsearchUtils.addData(JSONObject.parseObject(JSONObject.toJSONString(map)), "trs_test", "patent_test16", "id=" + i);
            i++;
        }
    }

    @Test
    public void trsTest() throws Exception {
        String[] columns = ("SYSID,申请号,申请日,公开（公告）号,公开（公告）日").split(",");
        String[] columns2 = ("权利要求书,说明书").split(",");
        TRSOperation trsOperation = new TRSOperation();
        List<Map<String, String>> list = trsOperation.queryForList("FMZL,FMSQ", "公开（公告）日>='2017.01.01'",
                columns , columns2,0,2);
        for (Map<String, String> map : list) {
            Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                System.out.println("[" + entry.getKey() + ":" + entry.getValue() + "]");
            }
            System.out.println("=====================================================================");
        }
    }

    @Test
    public void helloWorld() {

        System.out.println("Hello World!");
    }

}

























