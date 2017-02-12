package com.jana.karim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UserAdmin {

    private static HTable table;

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        TableName tableName = TableName.valueOf("User");
        table = new HTable(conf, tableName);

        if(args[0].equals("add")){
            System.out.println("Adding new user");
            String user_id = args[1];
            String email = args[2];
            String password = args[3];
            String status = args[4];
            String date = args[5];
            String security_question = args[6];
            String security_answer = args[7];

            add_new_user(user_id,email,password,status,date,security_question,security_answer);

        }
        else if(args[0].equals("delete")){
            String user_id = args[1];
            delete_user(user_id);
        }
        else if(args[0].equals("show")){
            String user_id = args[1];
            show_user(user_id);
        }
        else if(args[0].equals("listall")) {
            list_all_users();
        }
        else if(args[0].equals("login")){
            String user_id = args[1];
            String password = args[2];
            String ip = args[3];
            login(user_id,password,ip);
        }
    }


    public static void add_new_user(String user_id, String email, String password, String status,
                                    String dateOfBirth, String security_question, String security_answer) throws IOException {
        try {
            Put put = new Put(Bytes.toBytes(user_id));
            put.add(Bytes.toBytes("creds"), Bytes.toBytes("email"), Bytes.toBytes(email));
            put.add(Bytes.toBytes("creds"), Bytes.toBytes("password"), Bytes.toBytes(password));

            put.add(Bytes.toBytes("prefs"), Bytes.toBytes("status"), Bytes.toBytes(status));
            put.add(Bytes.toBytes("prefs"), Bytes.toBytes("date_of_birth"), Bytes.toBytes(dateOfBirth));
            put.add(Bytes.toBytes("prefs"), Bytes.toBytes("security_question"), Bytes.toBytes(security_question));
            put.add(Bytes.toBytes("prefs"), Bytes.toBytes("security_answer"), Bytes.toBytes(security_answer));

            table.put(put);
        }finally {
            table.close();
        }

    }

    public static void delete_user(String user_id) throws IOException {
        try{
            Delete delete = new Delete(Bytes.toBytes(user_id));
            table.delete(delete);
        }finally {
            table.close();
        }
    }

    public static void show_user(String user_id) throws IOException {
        try {
            Get get = new Get(Bytes.toBytes(user_id));
            Result result = table.get(get);
            print(result);

        }finally {
            table.close();
        }
    }
    public static void list_all_users() throws IOException {
        try {
            Scan scan = new Scan();
            ResultScanner result_scanner = table.getScanner(scan);
            for (Result result : result_scanner) {
                print(result);
            }
        }finally {
            table.close();
        }
    }
    public static void login(String user_id, String password, String ip) throws IOException {
        try {
            Get get = new Get(Bytes.toBytes(user_id));
            Result result = table.get(get);
            //update records
            Put put = new Put(Bytes.toBytes(user_id));
            Date dateObj = new Date();

            SimpleDateFormat datef = new SimpleDateFormat("yyyy/MM/dd");
            SimpleDateFormat timef = new SimpleDateFormat("hh:mm:ss");

            String date = datef.format(dateObj);
            String time = timef.format(dateObj);

            put.add(Bytes.toBytes("lastlogin"), Bytes.toBytes("ip"),Bytes.toBytes(ip));
            put.add(Bytes.toBytes("lastlogin"), Bytes.toBytes("date"),Bytes.toBytes(date));
            put.add(Bytes.toBytes("lastlogin"), Bytes.toBytes("time"),Bytes.toBytes(time));
            // check password
            String user_pass = Bytes.toString( result.getValue(Bytes.toBytes("creds"), Bytes.toBytes("password")) );
            if(password.equals(user_pass)){
                //success
                put.add(Bytes.toBytes("lastlogin"), Bytes.toBytes("success"),Bytes.toBytes("yes"));
            }
            else{
                put.add(Bytes.toBytes("lastlogin"), Bytes.toBytes("success"),Bytes.toBytes("no"));
            }
            table.put(put);
        }finally {
            table.close();
        }
    }

    public static void print(Result result){

        System.out.println("rowid="+ Bytes.toString(result.getRow()));
        System.out.println("creds:email="+
                Bytes.toString( result.getValue(Bytes.toBytes("creds"), Bytes.toBytes("email")) ));
        System.out.println("creds:password="+
                Bytes.toString( result.getValue(Bytes.toBytes("creds"), Bytes.toBytes("password")) ));
        System.out.println("prefs:status="+
                Bytes.toString( result.getValue(Bytes.toBytes("prefs"), Bytes.toBytes("status")) ));
        System.out.println("prefs:date_of_birth="+
                Bytes.toString( result.getValue(Bytes.toBytes("prefs"), Bytes.toBytes("date_of_birth")) ));
        System.out.println("prefs:security_question="+
                Bytes.toString( result.getValue(Bytes.toBytes("prefs"), Bytes.toBytes("security_question")) ));
        System.out.println("prefs:security_answer="+
                Bytes.toString( result.getValue(Bytes.toBytes("prefs"), Bytes.toBytes("security_answer")) ));
        System.out.println("last_login:ip="+
                Bytes.toString( result.getValue(Bytes.toBytes("lastlogin"), Bytes.toBytes("ip")) ));
        System.out.println("last_login:date="+
                Bytes.toString( result.getValue(Bytes.toBytes("lastlogin"), Bytes.toBytes("date")) ));
        System.out.println("last_login:time=" +
                Bytes.toString( result.getValue(Bytes.toBytes("lastlogin"), Bytes.toBytes("time")) ));
        System.out.println("last_login:success=" +
                Bytes.toString( result.getValue(Bytes.toBytes("lastlogin"), Bytes.toBytes("success")) ));


    }

}
