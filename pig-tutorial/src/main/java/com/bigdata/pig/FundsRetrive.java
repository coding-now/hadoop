package com.bigdata.pig;

import com.bigdata.util.HttpClientUtil;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by WangBin on 2017/8/21.
 */
public class FundsRetrive extends EvalFunc<String> {
    public String exec(Tuple tuple) throws IOException {
        if(tuple!=null){
            String code = (String)tuple.get(0);
            return getHistoryData(code);
        }
        return null;
    }
    private String getHistoryData(String code)throws IOException{
        String url = "http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code="
                .concat(code).concat("&page=1&per=1000&sdate=2015-08-01&edate=");
        String val = HttpClientUtil.httpGet(url);
        String body = val.substring(val.indexOf("<tbody>")+"<tbody>".length(),val.indexOf("</tbody>"));
        body = body.replaceAll("<tr>","\n").replaceAll("</tr>","\r");
        body = body.replaceAll("<td>"," ").replaceAll("</td>"," ");
        body = body.replaceAll("<td class='tor bold'>"," ")
                .replaceAll("<td class='tor bold red'>"," ")
                .replaceAll("<td class='tor bold grn'>"," ").replaceAll("<td class='.*'>"," ");
        System.out.println("==>"+body);
        return val;
    }
    public static void main(String[] args)throws Exception{
        String val = new FundsRetrive().getHistoryData("150008");
        //System.out.println("==>"+val);
    }
    @Override
    /**
     * This method gives a name to the column.
     * @param input - schema of the input data
     * @return schema of the output data
     */
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

        return funcList;
    }
}
