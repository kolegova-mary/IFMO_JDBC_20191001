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

import java.util.Comparator;
import java.util.List;


public class ServiceFactory {

    List<Department> departments = getAllDepartments();

    private List<Department> getAllDepartments() {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try {
            Connection connection = connectionSource.createConnection();
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
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

    private List<Employee> getAllEmployees() {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try {
            Connection connection = connectionSource.createConnection();
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE");
            return mapSet(resultSet);
        } catch (SQLException e) {
            return null;
        }
    }

    public Employee employeeRowMapper(ResultSet resultSet, boolean isFir) {
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

    private Employee getMan(ResultSet resultSet, Integer id) {
        try {
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

    private Department getDepartment(String department) {
        for (Department d : departments) {
            if (d.getId().toString().equals(department)) {
                return d;
            }
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

    List<Employee> getRightPage(Paging paging, List<Employee> employees) {
        int a = employees.size();
        if ((paging.page) * paging.itemPerPage < a) {
            a = (paging.page) * paging.itemPerPage;
        }
        return employees.subList(paging.itemPerPage * (paging.page - 1), a);
    }


    public EmployeeService employeeService() {
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                List<Employee> ans = getAllEmployees();
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getHired().compareTo(o2.getHired());
                    }
                });
                return getRightPage(paging, ans);
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                List<Employee> ans = getAllEmployees();
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                    }
                });
                return getRightPage(paging, ans);
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                List<Employee> ans = getAllEmployees();
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getSalary().compareTo(o2.getSalary());
                    }
                });
                return getRightPage(paging, ans);
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                List<Employee> ans = getAllEmployees();
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        if (o1.getDepartment() == null){
                            return -1;
                        }
                        if (o2.getDepartment() == null){
                            return 1;
                        }
                        if (o1.getDepartment().getName().equals(o2.getDepartment().getName())) {
                            return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                        } else {
                            return o1.getDepartment().getName().compareTo(o2.getDepartment().getName());
                        }

                    }
                });
                return getRightPage(paging, ans);
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                List<Employee> ans = getAllEmployees();
                ans.removeIf(employee -> employee.getDepartment() == null);
                ans.removeIf(employee -> !employee.getDepartment().getId().equals(department.getId()));
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getHired().compareTo(o2.getHired());
                    }
                });
                return getRightPage(paging, ans);
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                List<Employee> ans = getAllEmployees();
                ans.removeIf(employee -> employee.getDepartment() == null);
                ans.removeIf(employee -> !employee.getDepartment().getId().equals(department.getId()));
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getSalary().compareTo(o2.getSalary());
                    }
                });
                return getRightPage(paging, ans);
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                List<Employee> ans = getAllEmployees();
                ans.removeIf(employee -> employee.getDepartment() == null);
                ans.removeIf(employee -> !employee.getDepartment().getId().equals(department.getId()));
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                    }
                });
                return getRightPage(paging, ans);
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                List<Employee> ans = getAllEmployees();
                ans.removeIf(employee -> employee.getManager() == null);
                ans.removeIf(employee -> !employee.getManager().getId().equals(manager.getId()));
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                    }
                });
                return getRightPage(paging, ans);
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                List<Employee> ans = getAllEmployees();
                ans.removeIf(employee -> employee.getManager() == null);
                ans.removeIf(employee -> !employee.getManager().getId().equals(manager.getId()));
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getHired().compareTo(o2.getHired());
                    }
                });
                return getRightPage(paging, ans);
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                List<Employee> ans = getAllEmployees();
                ans.removeIf(employee -> employee.getManager() == null);
                ans.removeIf(employee -> !employee.getManager().getId().equals(manager.getId()));
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getSalary().compareTo(o2.getSalary());
                    }
                });
                return getRightPage(paging, ans);
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
                List<Employee> ans = getAllEmployees();
                ans.removeIf(employee -> employee.getDepartment() == null);
                ans.removeIf(employee -> !employee.getDepartment().getId().equals(department.getId()));
                ans.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o2.getSalary().compareTo(o1.getSalary());
                    }
                });
                return ans.get(salaryRank-1);
            }
        };
    }

    public Employee employeeRowMapperChain(ResultSet resultSet) {
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