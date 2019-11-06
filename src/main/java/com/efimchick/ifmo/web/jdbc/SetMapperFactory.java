package com.efimchick.ifmo.web.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public Employee employeeRowMapper(ResultSet resultSet) {
                try {
                    return new Employee(
                            new BigInteger(resultSet.getString("ID")),
                            new FullName(resultSet.getString("FIRSTNAME"),
                                    resultSet.getString("LASTNAME"),
                                    resultSet.getString("MIDDLENAME")),
                            Position.valueOf(resultSet.getString("POSITION")),
                            LocalDate.parse(resultSet.getString("HIREDATE")),
                            new BigDecimal(resultSet.getDouble("salary")),
                            getManager(resultSet, resultSet.getInt("MANAGER"))
                    );
                } catch (SQLException ignored){
                    return null;
                }
            }

    private Employee getManager(ResultSet resultSet, int idOfManager) {
        Employee manager = null;
        try {
            int n = resultSet.getRow();
            resultSet.beforeFirst();
            while (resultSet.next()) {
                if (resultSet.getInt("ID") == idOfManager) {
                    manager = employeeRowMapper(resultSet);
                    break;
                }
            }
            resultSet.absolute(n);
        } catch (SQLException e) {
        }
        return manager;
    }

    public SetMapper<Set<Employee>> employeesSetMapper() {
        SetMapper<Set<Employee>> setMapper = new SetMapper<Set<Employee>>() {
            @Override
            public Set<Employee> mapSet(ResultSet resultSet) {
                Set<Employee> employees = new HashSet<>();
                try{
                    resultSet.beforeFirst();
                    while (resultSet.next()){
                        employees.add(employeeRowMapper(resultSet));
                    }
                } catch (SQLException e){

                }
                return employees;
            }
        };
        return setMapper;
    }
}
