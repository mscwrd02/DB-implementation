# MySQL Database Implementation with Python

This project demonstrates the basic implementation of a MySQL database using Python. The focus is on executing fundamental SQL queries, including `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, and `DESCRIBE`. It also covers primary keys, foreign keys, and the use of the `WHERE` clause. The project is suitable for beginners looking to understand how to interact with MySQL databases through Python.

## Features

- **Basic SQL Queries:** Implementation of core SQL commands such as `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, and `DESCRIBE`.
- **Primary and Foreign Keys:** Demonstrates the use of primary keys and foreign keys for relational database management.
- **Data Types:** Utilizes common data types including `INT`, `VARCHAR/STRING`, and `DATE`.
- **Python Integration:** Uses Python to connect and interact with the MySQL database, demonstrating real-world application scenarios.

## Prerequisites

- Python 3.8~3.12
- MySQL server
- MySQL Connector for Python (`mysql-connector-python`)


## SQL Commands Examples

Below are examples of SQL commands covered in this project:

- **Create Table:**

    ```sql
    create table apply (
     s_id char (10) not null,
     l_id int not null,
     apply_date date,
     primary key (s_id, l_id),
     foreign key (s_id) references students (id),
     foreign key (l_id) references lectures (id)
    );
    ```

- **Insert Data:**

    ```sql
    INSERT INTO employees (name, position, hire_date)
    VALUES ('John Doe', 'Software Engineer', '2024-09-01');
    ```

- **Select Data:**

    ```sql
    SELECT * FROM employees WHERE position = 'Software Engineer';
    ```

- **Update Data:**

    ```sql
    UPDATE employees
    SET position = 'Senior Software Engineer'
    WHERE id = 1;
    ```

- **Delete Data:**

    ```sql
    DELETE FROM employees WHERE id = 1;
    ```

- **Drop Table:**

    ```sql
    DROP TABLE employees;
    ```



