package com.aura.bigdata.spark.scala.sql.entity;

public class Person {
    private String name;
    private String birthday;
    private String province;

    public Person() {
    }

    public Person(String name, String birthday, String province) {
        this.name = name;
        this.birthday = birthday;
        this.province = province;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", birthday='" + birthday + '\'' +
                ", province='" + province + '\'' +
                '}';
    }
}
