package com.bigdata.pig;

import com.bigdata.util.FundLoadUtil;
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
    public String getHistoryData(String code)throws IOException{
        String body = FundLoadUtil.loadByCode(code);
        return body;
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
