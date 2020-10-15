package com.wqj.flink1.pojo;


import com.wqj.flink1.base.Person;

import java.io.Serializable;

public class PersonJ extends Person implements Serializable {


    public PersonJ(int id, String name, int age) {
        super(id, name, age);
    }
    public PersonJ(Person person) {
        super(person.id(), person.name(), person.age());
    }

    @Override
    public Object productElement(int n) {
        return null;
    }

    @Override
    public int productArity() {
        return 0;
    }

    @Override
    public boolean canEqual(Object that) {
        return false;
    }

    @Override
    public boolean equals(Object that) {
        return false;
    }
}
