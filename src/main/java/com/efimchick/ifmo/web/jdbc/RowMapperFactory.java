package com.efimchick.ifmo.web.jdbc;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class RowMapperFactory {

    public RowMapper<Employee> employeeRowMapper() {
        RowMapper<Employee> rowMapper = new RowMapper<Employee>() {
            @Override
            public Employee mapRow(ResultSet resultSet){
                try {
                    return new Employee(
                            new BigInteger(resultSet.getString("ID")),
                            new FullName(resultSet.getString("FIRSTNAME"),
                                            resultSet.getString("LASTNAME"),
                                            resultSet.getString("MIDDLENAME")),
                            Position.valueOf(resultSet.getString("POSITION")),
                            LocalDate.parse(resultSet.getString("HIREDATE")),
                            new BigDecimal(resultSet.getDouble("salary"))
                    );
                } catch (SQLException ignored){
                    return null;
                }
            }
        };
        return rowMapper;
    }
}