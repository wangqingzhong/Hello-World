package com.handbi.mongodb;


import com.mongodb.*;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;

/**
 * 使用多线程运行加载三个万象城的gooagoo数据入库
 */
public class HiveDataUpdate implements Runnable{

    private static Logger logger = LoggerFactory.getLogger(HiveDataUpdate.class);

    private static final String IP = "10.73.4.47";
    private static final int PORT = 27017;
    private static final String USER = "root";
    private static final String password = "siyouyun";

    private static JSONArray _jsonArray = new JSONArray();

    public String HOST ;
    public String PORTS;
    public String USERNAME;
    public String pw;


    //把三个万象城的数据放到一个Json中
    static {
        //南宁万象城
        JSONObject nn = new JSONObject();
        nn.put("USER",USER);
        nn.put("password",password);
        nn.put("PORT",PORT);
        nn.put("IP",IP);

        //深圳天地
        JSONObject mixc = new JSONObject();
        mixc.put("USER","reado");
        mixc.put("password","reado");
        mixc.put("PORT",27017);
        mixc.put("IP","10.90.10.6");


        //西咸万象城
        JSONObject xn = new JSONObject();
        xn.put("USER","readonly");
        xn.put("password","readonly");
        xn.put("PORT",27017);
        xn.put("IP","10.89.80.72");

        //加载到一个array中
        _jsonArray.add(nn);
        _jsonArray.add(xn);
        _jsonArray.add(mixc);

    }

    public HiveDataUpdate(String name, String password, String hosts, String port){
        this.USERNAME = name;
        this.pw = password;
        this.HOST = hosts;
        this.PORTS = port;

    }

    public static void main(String[] args) throws UnknownHostException {

        for (int i = 0 ;i<_jsonArray.size();i++){
            JSONObject _jsonObect = JSONObject.fromObject(_jsonArray.get(i));
            HiveDataUpdate run = new HiveDataUpdate(_jsonObect.getString("USER"),
                                                     _jsonObect.getString("password"),
                                                     _jsonObect.getString("IP"),
                                                     _jsonObect.getString("PORT"));
            new Thread(run).start();
        }
        System.out.println("----end");

    }


    /**
     * 加载驱动 三个万象城的数据分别是深圳万象天地，南宁万象城，西咸万象城
     * @param
     * @return
     */






    //获取前一天时间
    public static Date getNextDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        date = calendar.getTime();
        System.out.println(date);
        return date;
    }

    @Override
    public void run()
    {
        //加载驱动
        try {
            loadDriver(this.USERNAME,this.pw,this.HOST,this.PORTS);
        } catch (UnknownHostException e) {
            logger.error("读取万象城数据信息，插入hbase数据异常",e);
            e.printStackTrace();
        }
    }

    public void loadDriver(String username,String password,String host,String port) throws UnknownHostException {
        System.out.println(username);
        Mongo mongo = new Mongo(host,Integer.parseInt(port));
        DB db = mongo.getDB("admin");
        //每次切库都要进行slave操作
        db.slaveOk();
        if (!db.authenticate(username, password.toCharArray())){
            System.out.println("连接MongoDB数据库,校验失败！");
            return;
        }else{
            System.out.println("连接MongoDB数据库,校验成功！");
            db = mongo.getDB("gag_bill");
            db.slaveOk();
        }

        Set<String> name = db.getCollectionNames();
        for (String names : name){
            System.out.println(names);
        }
        //获取表
        DBCollection goods = db.getCollection("billInfo");
        //时间格式化为今天0点
        //查询条件 $gte大于等于今天
        BasicDBObject gt = new BasicDBObject("$gte",getNextDay(new Date()));
        BasicDBObject queryObject = new BasicDBObject("createTime",gt);
        DBCursor cur = goods.find(queryObject);
        System.out.println(username+"=---"+cur.itcount());
        while (cur.hasNext()) {
            //数据
            JSONObject jsonObject = JSONObject.fromObject(cur.next().toString());
            //rowkey
            String rowKey = jsonObject.getString("_id");
            //存入hbase
            try {
                //HbaseUtils.insertData("ods_gooagoo:check_detail","cf1",rowKey,jsonObject);
            } catch (Exception e) {
                logger.error("插入hbase异常，该次插入数据为："+jsonObject,e);
            }
            //System.out.println(cur.next());
        }
        cur.close();


    }












}