package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.*;

public class DaoFactory {


    private List<Department> mapSetDepartments(ResultSet resultSet) throws SQLException {
        resultSet.beforeFirst();
        List<Department> departments = new ArrayList<>();
        try {
            while (resultSet.next()) {
                departments.add(
                        new Department(
                                new BigInteger(resultSet.getString("ID")),
                                resultSet.getString("NAME"),
                                resultSet.getString("LOCATION")
                        )
                );
            }
        } catch (SQLException e) {
        }
        return departments;
    }

    private ResultSet getResultSet(String SQL) throws SQLException {
        ConnectionSource connectionSource = ConnectionSource.instance();
        Connection connection = connectionSource.createConnection();
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        return statement.executeQuery(SQL);
    }

    private void insertAndDelete(String SQL) throws SQLException {
        ConnectionSource connectionSource = ConnectionSource.instance();
        Connection connection = connectionSource.createConnection();
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        statement.executeUpdate(SQL);
    }

    private Employee employeeRowMapper(ResultSet resultSet) {
        try {
            BigInteger managerId = BigInteger.ZERO;
            BigInteger departmentId = BigInteger.ZERO;
            if (resultSet.getString("MANAGER") != null) {
                managerId = new BigInteger(resultSet.getString("MANAGER"));
            }
            if (resultSet.getString("DEPARTMENT") != null) {
                departmentId = new BigInteger(resultSet.getString("DEPARTMENT"));
            }
            return new Employee(
                    new BigInteger(resultSet.getString("ID")),
                    new FullName(resultSet.getString("FIRSTNAME"),
                            resultSet.getString("LASTNAME"),
                            resultSet.getString("MIDDLENAME")),
                    Position.valueOf(resultSet.getString("POSITION")),
                    LocalDate.parse(resultSet.getString("HIREDATE")),
                    new BigDecimal(resultSet.getDouble("SALARY")),
                    managerId,
                    departmentId
            );
        } catch (SQLException ignored) {
            return null;
        }
    }

    private List<Employee> mapSet(ResultSet resultSet) throws SQLException {
        resultSet.beforeFirst();
        List<Employee> employees = new ArrayList<>();
        try {
            while (resultSet.next()) {
                employees.add(employeeRowMapper(resultSet));
            }
        } catch (SQLException e) {
        }
        return employees;
    }


    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                String sql = "select * from employee where department = " + department.getId();
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                String sql = "select * from employee where manager = " + employee.getId();
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                String sql = "select * from employee where id = " + Id;
                try {
                    ResultSet resultSet = getResultSet(sql);
                    if (resultSet.next() == false) {
                        return Optional.empty();
                    } else {
                        List<Employee> employees = mapSet(resultSet);
                        return Optional.of(employees.get(0));
                    }
                } catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Employee> getAll() {
                String sql = "select * from employee";
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Employee save(Employee employee) {
                String sql = "insert into employee values('"
                        + employee.getId() + "', '"
                        + employee.getFullName().getFirstName() + "', '"
                        + employee.getFullName().getLastName() + "', '"
                        + employee.getFullName().getMiddleName() + "', '"
                        + employee.getPosition() + "', '"
                        + employee.getManagerId() + "', '"
                        + employee.getHired() + "', '"
                        + employee.getSalary() + "', '"
                        + employee.getDepartmentId() + "')";
                try {
                    insertAndDelete(sql);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                String sql = "delete from employee where id = " + employee.getId();
                try {
                    insertAndDelete(sql);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                String sql = "select * from department where id = " + Id;
                try {
                    ResultSet resultSet = getResultSet(sql);
                    if (resultSet.next() == false) {
                        return Optional.empty();
                    } else {
                        List<Department> departments = mapSetDepartments(resultSet);
                        return Optional.of(departments.get(0));
                    }
                } catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Department> getAll() {
                String sql = "select * from department";
                try {
                    ResultSet resultSet = getResultSet(sql);
                    List<Department> departments = mapSetDepartments(resultSet);
                    return departments;
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Department save(Department department) {
                String sql;
                Optional<Department> optionalDepartment = getById(department.getId());
                if (optionalDepartment.isPresent()) {
                    sql = "update department set name = '"
                            + department.getName() + "',"
                            + " location = '" + department.getLocation()
                            + "' where id = '" + department.getId() + "'";
                } else {
                    sql = "insert into department values('"
                            + department.getId() + "', '"
                            + department.getName() + "', '"
                            + department.getLocation() + "')";
                }
                try {
                    insertAndDelete(sql);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return department;
            }

            @Override
            public void delete(Department department) {
                String sql = "delete from department where id = " + department.getId();
                try {
                    insertAndDelete(sql);
                }catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }
}