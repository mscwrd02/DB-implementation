# Library Management Application

This project implements a library management application using Python and MySQL. The application simulates the process of book borrowing, including managing books and users, borrowing and returning books, and rating books. This project helps to understand how to integrate an application with a relational database, focusing on SQL commands and database design principles.

## Features

- **Database Initialization:** Initialize the database with given data from a CSV file (`data.csv`). The data includes book and member information, such as book ratings given by members.
- **Manage Books and Users:** Add or remove books and users. Each book and user is uniquely identified by an ID with `AUTO_INCREMENT`.
- **Book Borrowing and Returning:** Users can borrow books, return them, and rate them. Borrowing is limited to 2 books per user.
- **Search and Recommend Books:** Search for books by title and recommend books to users based on popularity and collaborative filtering techniques.

## Prerequisites

- Python 3.8+
- MySQL server
- MySQL Connector for Python (`mysql-connector-python`)
- Additional Python libraries: `numpy`, `pandas` (specified in `requirements.txt`)



## Usage

1. Run the application using the command:

    ```bash
    python run.py
    ```

2. Follow the menu prompts to interact with the application:

    ```
    ============================================================
    1. Initialize database
    2. Print all books
    3. Print all users
    4. Insert a new book
    5. Remove a book
    6. Insert a new user
    7. Remove a user
    8. Check out a book
    9. Return and rate a book
    10. Print borrowing status of a user
    11. Search books
    12. Recommend a book for a user using popularity-based method
    13. Recommend a book for a user using user-based collaborative filtering
    14. Exit
    15. Reset database
    ============================================================
    ```

## Key SQL Commands Used

- **Create Table:**
    ```sql
    CREATE TABLE books (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(50),
        author VARCHAR(30),
        avg_rating FLOAT,
        quantity INT
    );
    ```
- **Insert Data:**
    ```sql
    INSERT INTO books (title, author, avg_rating, quantity)
    VALUES ('The Matrix', 'Lana Wachowski', 4, 1);
    ```
- **Select Data:**
    ```sql
    SELECT * FROM books WHERE title LIKE '%Matrix%';
    ```
- **Update Data:**
    ```sql
    UPDATE books SET quantity = quantity - 1 WHERE id = 1;
    ```
- **Delete Data:**
    ```sql
    DELETE FROM books WHERE id = 1;
    ```

## Project Structure

- `src/`: Contains Python scripts for the application logic and database interaction.
- `data.csv`: Initial dataset for populating the database.
- `requirements.txt`: List of Python dependencies.

## Database Design

The database schema includes tables for books, users, and borrowing records. The design ensures that each book and user has a unique identifier, and borrowing records link books and users with constraints to prevent borrowing of unavailable books or deletion of users/books involved in active borrowings.

