package com.factory.dynamicfactory.source.http;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dbutil.OkHttp3Client;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

public class HttpRichSourceFunction<RowData> extends RichParallelSourceFunction<RowData> {
    private ReadableConfig options;
    private List<RowType.RowField> schema;
    public String httpURL;
    public HttpRichSourceFunction(ReadableConfig options, DataType schema) {
        this.options = options;
        this.schema = ((RowType) (schema.getLogicalType())).getFields();
        httpURL = options.get(HttpSourceFactory.HTTPURL);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        while (true) {
            // 拉取http结果，将json结果和schema对应起来
            String toDay =  DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss");;
            String reqJson = "{\"startDate\":\""+toDay +"\",\"endDate\":\""+ toDay +"\"}";
            JSONArray jsonArr = OkHttp3Client.getAppinfos(httpURL, reqJson);
            if(jsonArr!=null){
                jsonArr.forEach(j ->{
                    JSONObject json = JSON.parseObject(j.toString());
                    GenericRowData row = new GenericRowData(schema.size());
                    for (int i = 0; i < schema.size(); i++) {
                        switch (schema.get(i).getType().getClass().getName()){
                            case "org.apache.flink.table.types.logical.VarCharType" :
                                if(json.containsKey(schema.get(i).getName())){
                                    row.setField(i, StringData.fromString(json.getString(schema.get(i).getName())));
                                } else {
                                    row.setField(i, StringData.fromString("-1"));
                                }
                                break;
                        }
                    }
                    sourceContext.collect((RowData) row);
                });
            }
            Thread.sleep(600000); // 10min
        }
    }

    @Override
    public void cancel() {

    }
}
