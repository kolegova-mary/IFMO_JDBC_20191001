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

    List<Employee> employees = getAllEmployees();
    List<Department> departments = getAllDepartments();

    private List<Department> getAllDepartments() {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try {
            Connection connection = connectionSource.createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM DEPARTMENT");
            return mapSetDepartments(resultSet);
        } catch (SQLException e) {
            return null;
        }
    }

    private List<Department> mapSetDepartments(ResultSet resultSet) {
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

    private static List<Employee> getAllEmployees() {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try {
            Connection connection = connectionSource.createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE");
            return mapSet(resultSet);
        } catch (SQLException e) {
            return null;
        }
    }

    public static Employee employeeRowMapper(ResultSet resultSet) {
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

    public static List<Employee> mapSet(ResultSet resultSet) {
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
                List<Employee> result = new ArrayList<>();
                for (Employee e : employees) {
                    if (e.getDepartmentId().equals(department.getId())) {
                        result.add(e);
                    }
                }
                return result;
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                List<Employee> result = new ArrayList<>();
                for (Employee e : employees) {
                    if (e.getManagerId().equals(employee.getId())) {
                        result.add(e);
                    }
                }
                return result;
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                for (Employee e : employees) {
                    if (e.getId().equals(Id)) {
                        return Optional.of(e);
                    }
                }
                return Optional.empty();
            }

            @Override
            public List<Employee> getAll() {
                return employees;
            }

            @Override
            public Employee save(Employee employee) {
                employees.add(employee);
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                employees.remove(employee);
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                for (Department d : departments) {
                    if (d.getId().equals(Id)) {
                        return Optional.of(d);
                    }
                }
                return Optional.empty();
            }

            @Override
            public List<Department> getAll() {
                return departments;
            }

            @Override
            public Department save(Department department) {
                departments.removeIf(d -> d.getId().equals(department.getId()));
                departments.add(department);
                return department;
            }

            @Override
            public void delete(Department department) {
                departments.remove(department);
            }
        };
    }
}