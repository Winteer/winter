package io.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseClient {
    public static final String TABLE = "xingoo:test_v";

    private static Configuration conf = null;
    private static Connection conn = null;

    static {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.quorum", "hbase1,hbase2,hbase3,hbase4");
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据表名，rowKey，列族，Map List 进行写入
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @param tmpList
     * @throws IOException
     */
    public void write(String tableName,String rowKey,String colFamily,List<Map<String,String>> tmpList) throws IOException {
        List<Put> puts = new ArrayList();
        HTable myTable = new HTable(conf, TableName.valueOf(tableName));
        myTable.setAutoFlush(false, false);
        myTable.setWriteBufferSize(3 * 1024 * 1024);
        //可以自己设置时间戳作为版本号，也可以使用默认时间
//        p.addColumn(Bytes.toBytes("v"), Bytes.toBytes("c1"), System.currentTimeMillis(), Bytes.toBytes("test1"));
        for(Map<String,String> tmpMap : tmpList){
            Put p = new Put(Bytes.toBytes(rowKey));
            for (Map.Entry<String, String> entry : tmpMap.entrySet()) {
//                System.out.println(entry.getKey() + ":" + entry.getValue());
                p.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            }
            puts.add(p);
        }
        myTable.put(puts);
        myTable.flushCommits();
        myTable.close();
    }

    /**
     * 根据表名，列族，rowkey进行读取
     * @param tableName
     * @param colFamily
     * @param filter
     * @return
     * @throws IOException
     */
    public List<Map<String,String>> read(String tableName, String colFamily, String filter) throws IOException {
        Table table = HbaseClient.conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
//        scan.addColumn("v".getBytes(),"c1".getBytes());
        scan.addFamily(colFamily.getBytes());
        scan.setMaxVersions(1);//设置读取的最大的版本数
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(filter.getBytes()));//基于过滤器设置查询条件
        scan.setFilter(rowFilter);
        ResultScanner r = table.getScanner(scan);
        List<Map<String,String>> list = new ArrayList<>();
        for(Result result : r) {
            for (KeyValue kv : result.raw()) {
                Map<String,String> tmpMap = new HashMap();
//                System.out.println(Bytes.toString(kv.getRow())); //rowkey
//                System.out.println(Bytes.toString(kv.getQualifier())); //colName
                tmpMap.put(Bytes.toString(kv.getQualifier()),Bytes.toString(kv.getValue()));
                list.add(tmpMap); //value
            }
        }
//        System.out.println(list.size());
        System.out.println(list.toString());
        table.close();
        return list;
    }

    public static void QueryByCondition2(String tableName) {

        try {
            Table table = HbaseClient.conn.getTable(TableName.valueOf(TABLE));
            Scan scan = new Scan();
//        scan.addColumn("v".getBytes(),"c1".getBytes());
            scan.addFamily("v".getBytes());
            scan.setMaxVersions(1);//设置读取的最大的版本数
            Filter filter = new SingleColumnValueFilter(Bytes
                    .toBytes("c1"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("value6")); // 当列column1的值为aaa时进行查询
            Scan s = new Scan();
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println(keyValue.toString());
                    System.out.println("列：" + new String(keyValue.getFamily())+":"+Bytes.toString(keyValue.getQualifier())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HbaseClient client = new HbaseClient();
        List<Map<String,String>> tmpList = new ArrayList<>();
        Map<String,String> tmpMap = new HashMap();
        tmpMap.put("photo1","thisisaphoto1");
        tmpMap.put("anotherphoto1","thisisaanotherphoto1");
        tmpList.add(tmpMap);
        try {
            client.read("xingoo:test_v","v","1");
//            client.QueryByCondition2(TABLE);
//            client.write("xingoo:test_v","yyy","v",tmpList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
