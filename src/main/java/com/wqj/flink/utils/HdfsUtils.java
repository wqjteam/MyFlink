package com.wqj.flink.utils;

import java.io.File;

public class HdfsUtils {
    public static void main(String[] args) {
        System.out.println(isFlock());
    }

    public static Boolean isFlock() {
        File file = new File("/opt/module/");
        if (file.exists()) {
            return true;
        } else {
            return false;
        }
    }

    public static Boolean isDevFlock() {
        File file = new File("/opt/module/");
        if (file.exists()) {
            return true;
        } else {
            return false;
        }
    }

    public static Boolean isTestFlock() {
        File file = new File("/opt/module/");
        if (file.exists()) {
            return true;
        } else {
            return false;
        }
    }

    public static Boolean isPrdFlock() {
        File file = new File("/opt/module/");
        if (file.exists()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 返回配置文件的路径
     * */
//    public static getHadoopConf(): String = {
//        var rootPath = OperateHbase.getClass.getResource("").getPath.replaceAll("classes\\S{1,100}", "classes/")
//        if(rootPath.startsWith("/")) rootPath=rootPath.replaceFirst("/","")
//        val filePth = "/etc/hadoop/conf"
//        val file = new File(filePth)
//        if (!file.exists) { // 文件不存在
//            return rootPath
//        }
//        return filePth
//    }
}
