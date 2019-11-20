package com.efimchick.ifmo.web.jdbc.service;

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
import java.util.ArrayList;
import java.util.List;

public class ServiceFactory {

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


    private Employee employeeRowMapper(ResultSet resultSet, boolean isFir) {
        try {
            Employee manager = null;
            Department department = null;
            if (resultSet.getString("MANAGER") != null) {
                if (isFir)
                    manager = getMan(resultSet, resultSet.getInt("MANAGER"));
            }

            if (resultSet.getString("DEPARTMENT") != null) {
                department = getDepartment(resultSet.getString("DEPARTMENT"));
            }

            return new Employee(
                    new BigInteger(resultSet.getString("ID")),
                    new FullName(resultSet.getString("FIRSTNAME"),
                            resultSet.getString("LASTNAME"),
                            resultSet.getString("MIDDLENAME")),
                    Position.valueOf(resultSet.getString("POSITION")),
                    LocalDate.parse(resultSet.getString("HIREDATE")),
                    new BigDecimal(resultSet.getDouble("SALARY")),
                    manager,
                    department
            );
        } catch (SQLException ignored) {
            return null;
        }
    }

    private Employee getMan(ResultSet res, Integer id) {
        try {
            ResultSet resultSet = getResultSet("select * from employee");
            Employee man = null;
            int row = resultSet.getRow();
            resultSet.beforeFirst();
            while (resultSet.next()) {
                if (resultSet.getString("ID").equals(String.valueOf(id))) {
                    man = employeeRowMapper(resultSet, false);
                    break;
                }
            }
            resultSet.absolute(row);
            return man;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    private List<Employee> mapSet(ResultSet resultSet) {
        List<Employee> employees = new ArrayList<>();
        try {
            while (resultSet.next()) {
                employees.add(employeeRowMapper(resultSet, true));
            }
        } catch (SQLException ignored) {
        }
        return employees;
    }

    private ResultSet getResultSet(String SQL) throws SQLException {
        ConnectionSource connectionSource = ConnectionSource.instance();
        Connection connection = connectionSource.createConnection();
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        return statement.executeQuery(SQL);
    }

    public EmployeeService employeeService() {
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                String sql = "select * from employee order by hiredate"+
                        " limit " + paging.itemPerPage +
                        " offset " + paging.itemPerPage * (paging.page - 1);
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                String sql = "select * from employee order by lastname"+
                        " limit " + paging.itemPerPage +
                        " offset " + paging.itemPerPage * (paging.page - 1);
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                String sql = "select * from employee order by salary"+
                        " limit " + paging.itemPerPage +
                        " offset " + paging.itemPerPage * (paging.page - 1);
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                String sql = "select * from employee order by department,lastname"+
                        " limit " + paging.itemPerPage +
                        " offset " + paging.itemPerPage * (paging.page - 1);
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                String sql = "select * from employee where department = " + department.getId() +" order by hiredate"+
                        " limit " + paging.itemPerPage +
                        " offset " + paging.itemPerPage * (paging.page - 1);
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                String sql = "select * from employee where department = " + department.getId() +" order by salary"+
                        " limit " + paging.itemPerPage +
                        " offset " + paging.itemPerPage * (paging.page - 1);
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                String sql = "select * from employee where department = " + department.getId() +" order by lastname"+
                        " limit " + paging.itemPerPage +
                        " offset " + paging.itemPerPage * (paging.page - 1);
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                String sql = "select * from employee where manager = " + manager.getId() +" order by lastname"+
                        " limit " + paging.itemPerPage +
                        " offset " + paging.itemPerPage * (paging.page - 1);
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                String sql = "select * from employee where manager = " + manager.getId() +" order by hiredate"+
                        " limit " + paging.itemPerPage +
                        " offset " + paging.itemPerPage * (paging.page - 1);
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                String sql = "select * from employee where manager = " + manager.getId() +" order by salary"+
                        " limit " + paging.itemPerPage +
                        " offset " + paging.itemPerPage * (paging.page - 1);
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                ConnectionSource connectionSource = ConnectionSource.instance();
                Connection connection = null;
                try {
                    connection = connectionSource.createConnection();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                Statement statement = null;
                try {
                    statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                try {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE");
                    List<Employee> employees = new ArrayList<>();
                    while (resultSet.next()){
                        employees.add(employeeRowMapperChain(resultSet));
                    }
                    for (Employee e : employees){
                        if (e.getId().equals(employee.getId()))
                            return e;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                String sql = "select * from employee where department = " + department.getId() +" order by salary desc";
                try {
                    ResultSet resultSet = getResultSet(sql);
                    return mapSet(resultSet).get(salaryRank-1);
                } catch (SQLException e) {
                    return null;
                }
            }
        };
    }

    private Employee employeeRowMapperChain(ResultSet resultSet) {
        try {
            Employee manager = null;
            Department department = null;
            if (resultSet.getString("MANAGER") != null) {
                manager = getManChain(resultSet, resultSet.getInt("MANAGER"));
            }

            if (resultSet.getString("DEPARTMENT") != null) {
                department = getDepartment(resultSet.getString("DEPARTMENT"));
            }

            return new Employee(
                    new BigInteger(resultSet.getString("ID")),
                    new FullName(resultSet.getString("FIRSTNAME"),
                            resultSet.getString("LASTNAME"),
                            resultSet.getString("MIDDLENAME")),
                    Position.valueOf(resultSet.getString("POSITION")),
                    LocalDate.parse(resultSet.getString("HIREDATE")),
                    new BigDecimal(resultSet.getDouble("SALARY")),
                    manager,
                    department
            );
        } catch (SQLException ignored) {
            return null;
        }
    }

    private Department getDepartment(String department) {
        try {
            ResultSet res = getResultSet("select * from department where id = " + department);
            return mapSetDepartments(res).get(0);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Employee getManChain(ResultSet resultSet, Integer id) {
        try {
            Employee man = null;
            int row = resultSet.getRow();
            resultSet.beforeFirst();
            while (resultSet.next()) {
                if (resultSet.getString("ID").equals(String.valueOf(id))) {
                    man = employeeRowMapperChain(resultSet);
                    break;
                }
            }
            resultSet.absolute(row);
            return man;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
