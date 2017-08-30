package com.bigdata.util;

import java.io.IOException;

/**
 * Created by WangBin on 2017/8/25.
 */
public class FundLoadUtil {
    /*净值日期	单位净值	累计净值	日增长率	申购状态	赎回状态	分红送配*/
    public static String loadByCode(String code)throws IOException {
        String url = "http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code="
                .concat(code).concat("&page=1&per=1000&sdate=2015-08-01&edate=");
        System.out.println("-fund.eastmoney===>" + url);
        String val = HttpClientUtil.doPost(url,null,"utf-8");
        String body = val.substring(val.indexOf("<tbody>") + "<tbody>".length(),
                val.indexOf("</tbody>"));
        body = body.replaceAll("<tr>", "\n" + code).replaceAll("</tr>", "\r");
        body = body.replaceAll("<tr class='.*'>","\n" + code).replaceAll("<span.*</span>","");
        body = body.replaceAll("<td>", " ").replaceAll("</td>", " ");
        body = body.replaceAll("<td class='tor bold'>", " ")
                .replaceAll("<td class='tor bold red'>", " ")
                .replaceAll("<td class='tor bold grn'>", " ")
                .replaceAll("<td class='.*'>", " ");
        System.out.println("==>" + body);
        return body;
    }


    /*just for test*/
    public static void main(String[] args)throws Exception{
        String val = "";
        //val = loadByCode("159005");//
        val = loadByCode("150008");
        /*val = "2/29";
        System.out.println(val.contains("/"));
        System.out.println("==>"+val.substring(0,val.indexOf("/")));*/
        System.out.println(val);
    }
}
