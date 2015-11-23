package net.coderland.example.hadoop.mr.kpi;

import com.recallq.parseweblog.ParseWebLog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
 * KPI Object
 */
public class KPI {
    private String remote_addr;// 记录客户端的ip地址
    private String remote_user;// 记录客户端用户名称,忽略属性"-"
    private String time_local;// 记录访问时间与时区
    private String scheme; // 记录协议名称
    private String host;
    private String request;// 记录请求的url与http协议
    private String request_body; //记录POST请求的内容
    private String status;// 记录请求状态；成功是200
    private String request_time;
    private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
    private String http_referer;// 用来记录从那个页面链接访问过来的
    private String http_user_agent;// 记录客户浏览器的相关信息
    private String http_x_forwarded_for;

    private boolean valid = true;// 判断数据是否合法

    private static String metaPattern = "$remote_addr - $remote_user [$time_local] \"$scheme\" \"$host\" \"$request\" " +
            "$request_body $status $request_time $body_bytes_sent \"$http_referer\" \"$http_user_agent\" \"$http_x_forwarded_for\"";

    private static KPI parser(String line) {
//        System.out.println(line);
//        KPI kpi = new KPI();
//        String[] arr = line.split("");
//        if (arr.length > 11) {
//            kpi.setRemote_addr(arr[0]);
//            kpi.setRemote_user(arr[1]);
//            kpi.setTime_local(arr[3].substring(1));
//            kpi.setRequest(arr[6]);
//            kpi.setStatus(arr[8]);
//            kpi.setBody_bytes_sent(arr[9]);
//            kpi.setHttp_referer(arr[10]);
//
//            if (arr.length > 12) {
//                kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
//            } else {
//                kpi.setHttp_user_agent(arr[11]);
//            }
//
//            if (Integer.parseInt(kpi.getStatus()) >= 400) {// 大于400，HTTP错误
//                kpi.setValid(false);
//            }
//        } else {
//            kpi.setValid(false);
//        }
//        return kpi;

        ParseWebLog parser = new ParseWebLog(metaPattern);
        Map<String, Object> result = parser.parseLogLine(line);
        KPI kpi = new KPI();
        kpi.setRemote_addr((String) result.get("remote_addr"));
        kpi.setRemote_user((String) result.get("remote_user"));
        kpi.setTime_local((result.get("time_local")).toString());
        kpi.setScheme(result.get("scheme").toString());
        kpi.setHost(result.get("host").toString());
        kpi.setRequest(result.get("request").toString());
        kpi.setRequest_body(result.get("request_body").toString());
        kpi.setStatus(result.get("status").toString());
        kpi.setRequest_time(result.get("request_time").toString());
        kpi.setBody_bytes_sent(result.get("body_bytes_sent").toString());
        kpi.setHttp_referer(result.get("http_referer").toString());
        kpi.setHttp_user_agent(result.get("http_user_agent").toString());
        kpi.setHttp_x_forwarded_for(result.get("http_x_forwarded_for").toString());

        return kpi;
    }

    /**
     * 按page的pv分类
     */
    public static KPI filterPVs(String line) {
        KPI kpi = parser(line);
        Set<String> pages = new HashSet<String>();
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");

        if (!pages.contains(kpi.getRequest())) {
            kpi.setValid(false);
        }
        return kpi;
    }

    /**
     * 按page的独立ip分类
     */
    public static KPI filterIPs(String line) {
        KPI kpi = parser(line);
        Set<String> pages = new HashSet<String>();
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");

        if (!pages.contains(kpi.getRequest())) {
            kpi.setValid(false);
        }
        
        return kpi;
    }

    /**
     * PV按浏览器分类
     */
    public static KPI filterBroswer(String line) {
        return parser(line);
    }
    
    /**
     * PV按小时分类
     */
    public static KPI filterTime(String line) {
        return parser(line);
    }
    
    /**
     * PV按访问域名分类
     */
    public static KPI filterDomain(String line){
        return parser(line);
    }

    @Override
    public String toString() {
        return "KPI{" +
                "remote_addr='" + remote_addr + '\'' +
                ", remote_user='" + remote_user + '\'' +
                ", time_local='" + time_local + '\'' +
                ", scheme='" + scheme + '\'' +
                ", host='" + host + '\'' +
                ", request='" + request + '\'' +
                ", request_body='" + request_body + '\'' +
                ", status='" + status + '\'' +
                ", request_time='" + request_time + '\'' +
                ", body_bytes_sent='" + body_bytes_sent + '\'' +
                ", http_referer='" + http_referer + '\'' +
                ", http_user_agent='" + http_user_agent + '\'' +
                ", http_x_forwarded_for='" + http_x_forwarded_for + '\'' +
                ", valid=" + valid +
                '}';
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public Date getTime_local_Date() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
        return df.parse(this.time_local);
    }
    
    public String getTime_local_Date_hour() throws ParseException{
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
        return df.format(this.getTime_local_Date());
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getRequest_time() {
        return request_time;
    }

    public void setRequest_time(String request_time) {
        this.request_time = request_time;
    }

    public String getHttp_x_forwarded_for() {
        return http_x_forwarded_for;
    }

    public void setHttp_x_forwarded_for(String http_x_forwarded_for) {
        this.http_x_forwarded_for = http_x_forwarded_for;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getRequest_body() {
        return request_body;
    }

    public void setRequest_body(String request_body) {
        this.request_body = request_body;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }
    
    public String getHttp_referer_domain(){
        if(http_referer.length()<8){ 
            return http_referer;
        }
        
        String str=this.http_referer.replace("\"", "").replace("http://", "").replace("https://", "");
        return str.indexOf("/")>0?str.substring(0, str.indexOf("/")):str;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public static void main(String args[]) {
//        String line = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 " +
//                "\"http://www.angularjs.cn/A00n\" " +
//                "\"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"";

//        String line = "114.255.44.132 - - [09/Jul/2015:14:23:25 +0800] \"https\" \"collector.app.kmail.com\" " +
//                "\"GET /reporter?aaaaa HTTP/1.1\" \"-\" \"- - -\" 200 0.000 26 \"-\" " +
//                "\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" \"-\"";

//        System.out.println(line);
//        KPI kpi = new KPI();
//        String[] arr = line.split("\\s+");
//
//        kpi.setRemote_addr(arr[0]);
//        kpi.setRemote_user(arr[1]);
//        kpi.setTime_local(arr[3].substring(1));
//        kpi.setRequest(arr[6]);
//        kpi.setStatus(arr[8]);
//        kpi.setBody_bytes_sent(arr[9]);
//        kpi.setHttp_referer(arr[10]);
//        kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
//        System.out.println(kpi);
//
//        System.out.println("***********************************");
//
//        try {
//            SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd:HH:mm:ss", Locale.US);
//            System.out.println(df.format(kpi.getTime_local_Date()));
//            System.out.println(kpi.getTime_local_Date_hour());
//            System.out.println(kpi.getHttp_referer_domain());
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }

        String line = "189.177.244.134 - - [18/Nov/2015:12:24:19 +0800] \"https\" \"collector-app.kmail.com\" \"POST /reporter HTTP/1.1\" {\\x22value\\x22:{\\x22module\\x22:\\x22WPSMAIL_EXCEPTION_01\\x22,\\x22desc\\x22:\\x22OAuthAuthenticationActivity.onLoadFinished()\\x22,\\x22stack\\x22:\\x22\\x22,\\x22type\\x22:-1,\\x22email\\x22:\\x22danjosguer@gmail.com\\x22,\\x22msg\\x22:\\x22Login Success!\\x22},\\x22uploadTime\\x22:1447820656836,\\x22uuid\\x22:\\x22acdd5500fe81f642\\x22,\\x22systemVersion\\x22:\\x224.4.2\\x22,\\x22channel\\x22:\\x22miui\\x22,\\x22version\\x22:\\x22MIUI_V7_EMAIL_20150828_b1\\x22} 200 0.001 27 \"-\" \"-\" \"-\"";
        KPI kpi = parser(line);

        System.out.println(kpi);
    }

}
