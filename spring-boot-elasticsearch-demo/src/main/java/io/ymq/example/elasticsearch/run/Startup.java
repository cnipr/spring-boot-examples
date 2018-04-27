package io.ymq.example.elasticsearch.run;

import com.alibaba.fastjson.JSONObject;
import io.ymq.example.elasticsearch.trs.TRSOperation;
import io.ymq.example.elasticsearch.utils.ElasticsearchUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.List;
import java.util.Map;

/**
 *
 */
@SpringBootApplication
@ComponentScan(value = {"io.ymq.example.elasticsearch"})
public class Startup {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Startup.class, args);

        trs2es();
	}

    private static void trs2es() throws Exception {
        String[] columns = ("SYSID,申请号,申请日,公开（公告）号,公开（公告）日").split(",");
        String[] columns2 = ("权利要求书,说明书").split(",");
        TRSOperation trsOperation = new TRSOperation();
        List<Map<String, String>> list = trsOperation.queryForList("FMZL,FMSQ", "公开（公告）日>='2017.01.01'",
                columns , columns2,0,5);
        int i = 0;
        for (Map<String, String> map : list) {
            ElasticsearchUtils.addData(JSONObject.parseObject(JSONObject.toJSONString(map)), "trs_test", "patent_test5", "id=" + i);
            i++;
        }
    }
}
