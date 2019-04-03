package cn.jxufe.czk.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import cn.jxufe.czk.entity.TwoTuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;


public class HBaseUtil {

    private static Configuration conf = null;  //用来读取配置文件
    static{
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "master");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        conf = new HBaseConfiguration(HBASE_CONFIG);
    }

    public static Connection getConnection() throws Exception{
        return ConnectionFactory.createConnection(conf);
    }

    public static void closeConnection(Connection connection) throws Exception{
        if(connection!=null){
            connection.close();
        }
    }

    public static void putData(Connection conn,String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    public static void deleteData(Connection conn,String tableName, String rowKey, String columnFamily, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
        table.delete(delete);
    }

    public static void deleteData(Connection conn,String tableName, String rowKey, String columnFamily) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
    }

    public static void deleteData(Connection conn,String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    public static void scanData(Connection conn,String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner scan = table.getScanner(new Scan());
        for(Result result : scan){
            for(Cell cell : result.listCells()){
                System.out.println("行键："+ Bytes.toString(result.getRow())
                        + "\t 列族："+Bytes.toString(CellUtil.cloneFamily(cell))
                        + "\t 列："+Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "\t 值："+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void scanData(Connection conn,String tableName,String columnFamily) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner scan = table.getScanner(Bytes.toBytes(columnFamily));
        for(Result result : scan){
            for(Cell cell : result.listCells()){
                System.out.println("行键："+ Bytes.toString(result.getRow())
                        + "\t 列族："+Bytes.toString(CellUtil.cloneFamily(cell))
                        + "\t 列："+Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "\t 值："+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void scanData(Connection conn,String tableName,String columnFamily,String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner scan = table.getScanner(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
        for(Result result : scan){
            for(Cell cell : result.listCells()){
                System.out.println("行键："+ Bytes.toString(result.getRow())
                        + "\t 列族："+Bytes.toString(CellUtil.cloneFamily(cell))
                        + "\t 列："+Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "\t 值："+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    //读取一行数据（指定：表名——行键）
    public static Result getData(Connection conn,String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        return table.get(get);
    }
    //读取列族数据（指定：表名——行键——列族）
    public static Result getData(Connection conn,String tableName, String rowKey, String columnFamily) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(columnFamily));
        Result result = table.get(get);
        return result;
    }
    //读取单元格数据（指定：表名——行键——列族——列）
    public static String getData(Connection conn,String tableName, String rowKey, String columnFamily, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        Result result = table.get(get);
        String cellValue = Bytes.toString(result.value());
        return cellValue;
    }

    public static void describeTable(Connection conn,String tableName) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor htd = admin.getTableDescriptor(tn);
        System.out.println("=============表结构："+tableName+"=============");
        for(HColumnDescriptor hcd : htd.getColumnFamilies()){
            System.out.println(hcd.getNameAsString());
        }
        System.out.println("=============================================");
    }

    public static void deleteTable(Connection conn,String tableName) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if(admin.tableExists(tn)){
            //删除两步走
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
    }

    public static void deleteColumnFamily(Connection conn,String tableName, String...cfs) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if(admin.tableExists(tn)){
            HTableDescriptor htd = admin.getTableDescriptor(tn);
            for(String cf : cfs){
                htd.removeFamily(Bytes.toBytes(cf));
            }
            admin.modifyTable(tn,htd);   //执行修改表结构
        }
    }

    public static void addColumnFamily(Connection conn,String tableName, String...cfs) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if(admin.tableExists(tn)){
            HTableDescriptor htd = admin.getTableDescriptor(tn);
            for(String cf : cfs){
                HColumnDescriptor hcd = new HColumnDescriptor(cf);
                htd.addFamily(hcd);
            }
            admin.modifyTable(tn,htd);   //执行修改表结构
        }
    }

    public static void createTable(Connection conn,String tableName, String... cfs) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if(admin.tableExists(tn)){
            //删除两步走
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        //建表
        HTableDescriptor htd = new HTableDescriptor(tn);    //hbase 表的描述器
        for(String cf : cfs){
            HColumnDescriptor hcd = new HColumnDescriptor(cf);
            htd.addFamily(hcd);
        }
        admin.createTable(htd);   //执行建表
    }

    public static void listTables(Connection conn) throws IOException {
        Admin admin = conn.getAdmin();   //获取hbase的管理者，通过admin可以做DDL
        System.out.println("====================所有表格================");
        for(TableName tn : admin.listTableNames()){
            System.out.println(tn);
        }
        System.out.println("==========================================");
    }

    public static ArrayList<TwoTuple> getPreWordData(Connection conn,String tableName,String preWord) throws IOException {
        ArrayList<TwoTuple> list = new ArrayList<TwoTuple>();
        Table table = conn.getTable(TableName.valueOf(tableName));
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator((preWord+",").trim()));
        Scan scan = new Scan().setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner){
            for(Cell cell : result.listCells()){
                if(Bytes.toString(result.getRow()).startsWith(preWord)) {
                    TwoTuple twoTuple = new TwoTuple();
                    twoTuple.setTwoTuple(Bytes.toString(result.getRow()));
                    twoTuple.setNum(Long.parseLong(Bytes.toString(CellUtil.cloneValue(cell))));
                    list.add(twoTuple);
                }
            }
        }
        Collections.sort(list, (o1,o2)->{return (int) (o2.getNum()-o1.getNum());});
        return list;
    }

    public static void main(String[] args) throws IOException {
        //scanData("English", "info");
        //列出所有表
        //listTables();
        //创建表
        //createTable("English:video","info");
        //添加列族
        //addColumnFamily("student","account");
        //删除列族
        //deleteColumnFamily("student","account");
        //删除表
        //deleteTable("English");
        //查看表结构
        //describeTable("student");

        //====================表数据的增删改查===================
        //System.out.println(getData("user","xiaoming","info","company"));
		/*Result result = getData("user","xiaoming");
		for(Cell cell : result.listCells()){
			System.out.println("行键："+ Bytes.toString(result.getRow())
					+ "\t 列族："+Bytes.toString(CellUtil.cloneFamily(cell))
					+ "\t 列："+Bytes.toString(CellUtil.cloneQualifier(cell))
					+ "\t 值："+Bytes.toString(CellUtil.cloneValue(cell)));
		}*/
        //全表扫描
        //scanData("bigdata:movie");
        //删除数据
        //deleteData("user","zhangyifei");
        //deleteData("user","xiaoming","info");
        //deleteData("user","xiaoming","address","city");
        //System.out.println(getData("bigdata:movie","82558761048"));

        //添加数据
        //putData("user","xiaoming","info","company","alibaba");
    }
}
