package com.wqj.flink1.pojo;

public class RedisBasePojo {
    public String id;
    public String value;

    public RedisBasePojo(String id,String value){
        this.id=id;
        this.value=value;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "RedisBasePojo{" +
                "id='" + id + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
