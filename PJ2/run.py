from mysql.connector import connect
import pandas as pd
import numpy as np
connection = connect(
    host = 'astronaut.snu.ac.kr',
    port = 7001,
    user = 'DB2020_126960',
    password = 'DB2020_126960',
    db = 'DB2020_126960',
    charset = 'utf8'
)

TABLES = {}
TABLES['books'] = (
    "CREATE TABLE `books` ("
    "  `b_id` INT NOT NULL AUTO_INCREMENT,"
    "  `b_title` VARCHAR(50) NOT NULL,"
    "  `b_author` VARCHAR(30) NOT NULL,"
    "  `b_quantity` INT NOT NULL DEFAULT 1,"
    "  UNIQUE KEY `b_title_author` (`b_title`, `b_author`),"
    "  PRIMARY KEY (`b_id`)"
    ") ENGINE=InnoDB"
)
TABLES['users'] = (
    "CREATE TABLE `users` ("
    "  `u_id` INT NOT NULL AUTO_INCREMENT,"
    "  `u_name` VARCHAR(10) NOT NULL,"
    "  `u_borrowing` INT NOT NULL DEFAULT 0,"
    "  PRIMARY KEY (`u_id`)"
    ") ENGINE=InnoDB"
)
TABLES['borrowings'] = (
    "CREATE TABLE `borrowings` ("
    "  `b_id` INT NOT NULL,"
    "  `u_id` INT NOT NULL,"
    "  `rating` INT NULL,"
    "  `now` BOOLEAN NOT NULL DEFAULT 1,"
    "  PRIMARY KEY (`b_id`, `u_id`),"
    "  FOREIGN KEY (`b_id`) REFERENCES `books` (`b_id`),"
    "  FOREIGN KEY (`u_id`) REFERENCES `users` (`u_id`)"
    ") ENGINE=InnoDB"
)

#중복 사용되는 sql 쿼리문들
insert_book_with_id = (
    "INSERT INTO books (b_id,b_title, b_author) VALUES (%s, %s, %s)"
)
insert_book_with_title_author = ("INSERT INTO books (b_title, b_author) VALUES (%s, %s)")

find_book_with_id = ("SELECT * FROM books WHERE b_id = %s")

insert_user_with_id = (
    "INSERT INTO users (u_id, u_name) VALUES (%s,%s)"
)
insert_user_with_name = (
    "INSERT INTO users (u_name) VALUES (%s)"
)
find_user_with_id = ("SELECT * FROM users WHERE u_id = %s")
insert_borrowing_true = (
    "INSERT INTO borrowings (b_id, u_id) VALUES (%s, %s)"
)
insert_borrowing_false_with_rating = (
    "INSERT INTO borrowings (b_id, u_id, now, rating) VALUES (%s, %s, 0, %s)"
)
find_borrowing_with_ids = (
    "SELECT * FROM borrowings WHERE b_id = %s AND u_id = %s"
)

update_rating_and_return = (
    "UPDATE borrowings SET rating = %s, now=0 WHERE b_id = %s AND u_id = %s"
)

#Book ID를 입력받아 해당 도서가 존재하는지 확인
def is_book_exist(cursor, book_id):
    cursor.execute("SELECT * FROM books WHERE b_id = %s", (book_id,))

    if cursor.fetchone() == None:
        print("Book {} does not exist".format(book_id))
        return False
    return True
#User ID를 입력받아 해당 유저가 존재하는지 확인
def is_user_exist(cursor, user_id):
    cursor.execute("SELECT * FROM users WHERE u_id = %s", (user_id,))
    if cursor.fetchone() == None:
        print("User {} does not exist".format(user_id))
        return False
    return True

def initialize_database():
    with connection.cursor() as cursor:
        cursor.execute("USE DB2020_126960")
        #테이블을 생성한다.(처음에 initialize databse가 호출 될때는 테이블이 없다고 가정)
        for table_name in TABLES:
            cursor.execute(TABLES[table_name])

        # read data.csv and insert into books, users, borrowings
        df = pd.read_csv('data.csv', delimiter=',', encoding='euc-kr')

        for _, row in df.iterrows():
                #book을 찾아서 없으면 insert
                cursor.execute(find_book_with_id, (row['b_id'],))
                if cursor.fetchone() == None:
                    cursor.execute(insert_book_with_id, (row['b_id'], row['b_title'], row['b_author']))
                #user를 찾아서 없으면 insert
                cursor.execute(find_user_with_id, (row['u_id'],))
                if cursor.fetchone() == None:
                    cursor.execute(insert_user_with_id, (row['u_id'], row['u_name']))
                #borrowing을 찾아서 없으면 insert 있으면 rating업데이트
                cursor.execute(find_borrowing_with_ids, (row['b_id'], row['u_id']))
                if cursor.fetchone() == None:
                    cursor.execute(insert_borrowing_false_with_rating, (row['b_id'], row['u_id'], row['b_u_rating']))
                else: cursor.execute(update_rating_and_return, (row['b_u_rating'], row['b_id'], row['u_id']))
            
        connection.commit()
        print('Database successfully initialized')

    

def reset():

    print("정말로 데이터베이스를 리셋하시겠습니까?(y/n)")
    answer = input()
    if answer == 'n':
        print("reset 취소")
        return
    #테이블을 삭제하고, initialize_database를 호출한다(내부에서 테이블 생성 후 데이터 초기화)
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE borrowings")
        cursor.execute("DROP TABLE users")
        cursor.execute("DROP TABLE books")
        connection.commit()
    initialize_database()
    

def print_books():
    with connection.cursor() as cursor:
        cursor.execute("SELECT b.b_id, b_title, b_author, avg(rating) as avg_rating, b_quantity FROM books b left outer join borrowings r on b.b_id = r.b_id GROUP BY b.b_id ORDER BY b.b_id ASC")
        books = cursor.fetchall()
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"{'id':<5} {'title':<50} {'author':<30} {'avg.rating':<10} {'quantity':<1}")
        print("--------------------------------------------------------------------------------------------------------------")
        # 각 책 정보 출력
        for book in books:
            avg_rating = book[3] if book[3] != None else 'None'
            print(f"{book[0]:<5} {book[1]:<50} {book[2]:<30} {avg_rating:<10} {book[4]:<1}")
        print("--------------------------------------------------------------------------------------------------------------")

def print_users():
    with connection.cursor() as cursor:
        cursor.execute("SELECT u_id, u_name FROM users ORDER BY u_id ASC")
        users = cursor.fetchall()
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"{'id':<5} {'name':<10}")
        print("--------------------------------------------------------------------------------------------------------------")
        # 각 유저 정보 출력
        for user in users:
            print(f"{user[0]:<5} {user[1]:<10}")
        print("--------------------------------------------------------------------------------------------------------------")

    

def insert_book():
    #입력받기
    title = input('Book title: ')
    author = input('Book author: ')

    #title, author 길이 체크
    if len(title) < 1 or len(title) > 50:
        print("Title length should range from 1 to 50 characters")
        return
    if len(author) < 1 or len(author) > 30:
        print("Author length should range from 1 to 30 characters")
        return
    
    with connection.cursor() as cursor:
        #title, author 고유성 체크
        cursor.execute("SELECT * FROM books WHERE b_title = %s AND b_author = %s", (title, author))
        if cursor.fetchone() != None:
            print("Book ({}, {}) already exists".format(title, author))
            return
        
        cursor.execute(insert_book_with_title_author, (title, author))
        connection.commit()
        print("One book successfully inserted")

def remove_book():
    book_id = input('Book ID: ')

    with connection.cursor() as cursor:
        #book_id가 존재하는지 확인
        if not is_book_exist(cursor, book_id): return

        #book_id가 대출중인지 확인
        cursor.execute("SELECT * FROM borrowings WHERE b_id = %s AND now = 1", (book_id,))
        if cursor.fetchone() != None:
            print("Cannot delete a book that is currently borrowed")
            return
        
        #book_id의 borrowings, books에서 삭제
        cursor.execute("DELETE FROM borrowings WHERE b_id = %s", (book_id,))
        cursor.execute("DELETE FROM books WHERE b_id = %s", (book_id,))
        
        connection.commit()
    print("One book successfully removed")

def insert_user():
    name = input('User name: ')

    #name 길이 체크
    if len(name) < 1 or len(name) > 10:
        print("Username length should range from 1 to 10 characters")
        return
    
    with connection.cursor() as cursor:
        cursor.execute(insert_user_with_name, (name,))
        connection.commit()
    print("One user successfully inserted")

def remove_user():
    user_id = input('User ID: ')
    with connection.cursor() as cursor:

        #user_id가 존재하는지 확인
        if not is_user_exist(cursor, user_id): return

        #user_id가 대출중인지 확인
        cursor.execute("SELECT * FROM borrowings WHERE u_id = %s AND now = 1", (user_id,))
        if cursor.fetchone() != None:
            print("Cannot delete a user with borrowed books")
            return
        
        #user_id의 borrowings, users에서 삭제
        cursor.execute("DELETE FROM borrowings WHERE u_id = %s", (user_id,))
        cursor.execute("DELETE FROM users WHERE u_id = %s", (user_id,))
        
        connection.commit()
    print("One user successfully removed")

def checkout_book():
    book_id = input('Book ID: ')
    user_id = input('User ID: ')
    with connection.cursor() as cursor:
        #book_id, user_id가 존재하는지 확인
        if not is_book_exist(cursor, book_id) or not is_user_exist(cursor,user_id): return

        #book_id의 대출가능권수가 0이면 return
        cursor.execute("SELECT b_quantity FROM books WHERE b_id = %s", (book_id,))
        if cursor.fetchone()[0] == 0:
            print("Cannot check out a book that is currently borrowed")
            return
        
        #user_id의 대출한 권수 2이면-> return
        cursor.execute("SELECT u_borrowing FROM users WHERE u_id = %s", (user_id,))
        if cursor.fetchone()[0] == 2:
            print("User {} exceeded the maximum borrowing limit".format(user_id))
            return
        
        #book_id의 대출가능권수 -1, user_id의 대출한 권수 +1, borrowings에 추가
        cursor.execute("UPDATE books SET b_quantity = b_quantity - 1 WHERE b_id = %s", (book_id,))
        cursor.execute("UPDATE users SET u_borrowing = u_borrowing + 1 WHERE u_id = %s", (user_id,))
        cursor.execute("SELECT now FROM borrowings where b_id = %s and u_id = %s",(book_id,user_id))
        checkouted = cursor.fetchone()
        #대출이력이있으면, now만 업데이트
        if checkouted !=None :
            cursor.execute("UPDATE borrowings set now = 1 WHERE b_id = %s and u_id = %s", (book_id,user_id))
        else : cursor.execute(insert_borrowing_true, (book_id, user_id))
        connection.commit()
    print("Book successfully checked out")

def return_and_rate_book():
    book_id = input('book ID: ')
    user_id = input('User ID: ')
    rating = input('Ratings (1~5): ')

    #rating 형식 체크
    try:
        rating = int(rating)
    except:
        print("Rating should range from 1 to 5.")
        return
    if rating < 1 or rating > 5:
        print("Rating should range from 1 to 5.")
        return
    
    with connection.cursor() as cursor:
        #book_id, user_id가 존재하는지 확인
        if not is_book_exist(cursor, book_id) or not is_user_exist(cursor,user_id): return

        #user_id가 book_id를 대출중인지 확인
        cursor.execute("SELECT * FROM borrowings WHERE b_id = %s AND u_id = %s AND now = 1", (book_id, user_id))
        if cursor.fetchone() == None:
            print("Cannot return and rate a book that is not currently borrowed for this user")
            return
        
        #rating 업데이트하고 반납
        cursor.execute(update_rating_and_return, (rating, book_id, user_id))

        #book_id의 대출가능권수 +1, user_id의 대출한 권수 -1
        cursor.execute("UPDATE books SET b_quantity = b_quantity + 1 WHERE b_id = %s", (book_id,))
        cursor.execute("UPDATE users SET u_borrowing = u_borrowing - 1 WHERE u_id = %s", (user_id,))
        connection.commit()
    print("Book successfully returned and rated")


def print_borrowing_status_for_user():
    user_id = input('User ID: ')

    with connection.cursor() as cursor:
        #user_id가 존재하는지 확인
        if not is_user_exist(cursor, user_id): return
        #user_id가 대출중인 책들 출력
        cursor.execute("SELECT b.b_id, b_title, b_author, avg(rating) FROM books b left outer join borrowings r on b.b_id = r.b_id WHERE b.b_id IN (SELECT b_id FROM borrowings WHERE u_id = %s AND now = 1) GROUP BY b.b_id ORDER BY b.b_id", (user_id,))
        books = cursor.fetchall()
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"{'id':<5} {'title':<50} {'author':<30} {'avg.rating':<10}")
        print("--------------------------------------------------------------------------------------------------------------")
        # 각 책 정보 출력
        for book in books:
            avg_rating = book[3] if book[3] != None else 'None'
            print(f"{book[0]:<5} {book[1]:<50} {book[2]:<30} {avg_rating:<10}")
        print("--------------------------------------------------------------------------------------------------------------")


def search_books():
    query = input('Query: ')

    with connection.cursor() as cursor:
        # 검색어를 포함하는 도서 정보 출력(b_title, query모두 lower로 바꿔서 비교)
        cursor.execute("SELECT b.b_id, b_title, b_author, avg(rating), b_quantity FROM books b left outer join borrowings r on b.b_id = r.b_id WHERE LOWER(b_title) LIKE LOWER(%s) GROUP BY b.b_id ORDER BY b.b_id ASC", ('%'+query+'%',))
        books = cursor.fetchall()
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"{'id':<5} {'title':<50} {'author':<30} {'avg.rating':<10} {'quantity':<1}")
        print("--------------------------------------------------------------------------------------------------------------")
        # 각 책 정보 출력
        for book in books:
            avg_rating = book[3] if book[3] != None else 'None'
            print(f"{book[0]:<5} {book[1]:<50} {book[2]:<30} {avg_rating:<10} {book[4]:<1}")
        print("--------------------------------------------------------------------------------------------------------------")

def recommend_popularity():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    with connection.cursor() as cursor:
        if not is_user_exist(cursor, user_id): return
        #user가 평점을 남기지 않은 도서 중에, 평점 평균이 가장 높은 도서를 뽑는다.
        cursor.execute("""
    SELECT b.b_id, b_title, b_author, AVG(r.rating) AS avg_rating
    FROM books b
    LEFT OUTER JOIN borrowings r ON b.b_id = r.b_id
    WHERE b.b_id NOT IN (
        SELECT b_id
        FROM borrowings
        WHERE u_id = %s AND rating IS NOT NULL
    )
    GROUP BY b.b_id
    ORDER BY avg_rating DESC , b.b_id ASC
""", (user_id,))
        #limit문이 제대로 동작을 안해서, 전체를 fetch 한후에 첫번째를 선택한다.
        books = cursor.fetchall()
        
        print("--------------------------------------------------------------------------------------------------------------")
        print("Rating-based")
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"{'id':<5} {'title':<50} {'author':<30} {'avg.rating':<10}")
        print("--------------------------------------------------------------------------------------------------------------")
        if len(books) != 0 : 
            book = books[0]
            if book[3] == None : book[3] = 'None'
            print(f"{book[0]:<5} {book[1]:<50} {book[2]:<30} {book[3]:<10}")
        print("--------------------------------------------------------------------------------------------------------------")
        #user가 평점을 남기지 않은 도서 중에, 평점이 가장 많은 도서를 뽑는다.
        cursor.execute("""
    SELECT b.b_id, b_title, b_author, AVG(r.rating) AS avg_rating
    FROM books b
    LEFT OUTER JOIN borrowings r ON b.b_id = r.b_id
    WHERE rating IS NOT NULL 
      AND b.b_id NOT IN (
        SELECT b_id
        FROM borrowings
        WHERE u_id = %s AND rating IS NOT NULL
      )
    GROUP BY b.b_id
    ORDER BY COUNT(*) DESC, b.b_id ASC
""", (user_id,))
        books = cursor.fetchall()
        print("Popularity-based")
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"{'id':<5} {'title':<50} {'author':<30} {'avg.rating':<10}")
        print("--------------------------------------------------------------------------------------------------------------")
        if len(books) != 0 : 
            book = books[0]
            if book[3] == None : book[3] = 'None'
            print(f"{book[0]:<5} {book[1]:<50} {book[2]:<30} {book[3]:<10}")
        print("--------------------------------------------------------------------------------------------------------------")
        

def recommend_item_based():
    user_id = input('User ID: ')

    with connection.cursor() as cursor:
        if not is_user_exist(cursor, user_id): return
        #user id를 int로 변환
        try:
            user_id = int(user_id)
        except ValueError:
            print("Please input integer")
            return
        
        # 사용자 및 도서 ID 리스트 로드
        cursor.execute("SELECT u_id FROM users")
        users = cursor.fetchall()
        user_list = [user[0] for user in users]
        
        cursor.execute("SELECT b_id FROM books")
        books = cursor.fetchall()
        book_list = [book[0] for book in books]
        
        user_number = len(user_list)
        book_number = len(book_list)
        
        # User-Item Matrix 생성
        user_item_matrix = np.zeros((user_number, book_number))
        predict_ratings = np.zeros((book_number))
        
        cursor.execute("SELECT u_id, b_id, rating FROM borrowings WHERE rating IS NOT NULL")
        ratings = cursor.fetchall()
        for rating in ratings:
            user_index = user_list.index(rating[0])
            book_index = book_list.index(rating[1])
            user_item_matrix[user_index][book_index] = rating[2]
            if rating[0] == user_id:
                predict_ratings[book_index] = -1  # 이미 읽은 책은 -1로 설정
        
        #predict_ratings에 -1이 하나도 없으면, 그 유저는 모든 책을 읽지 않았다는 것인데
        # 그럼 모든 유저와 코사인 유사도가 0이 나올거니까, 가장 첫번째 책을 추천하면 된다.
        if -1 not in predict_ratings:
            print("--------------------------------------------------------------------------------------------------------------")
            print(f"{'id':<5} {'title':<50} {'author':<30} {'avg.rating':<10} {'exp.rating':<10}")
            print("--------------------------------------------------------------------------------------------------------------")
            cursor.execute("SELECT b.b_id, b_title, b_author,AVG(rating) FROM books b left outer JOIN borrowings r on b.b_id = r.b_id GROUP BY b.b_id ORDER BY b.b_id ASC")
            books = cursor.fetchall()
            if len(books) !=0:
                book = books[0]
                if book[3] == None : book[3] = 'None'
                print(book[0], book[1], book[2], book[3])
            print("--------------------------------------------------------------------------------------------------------------")
            return
        
        # 책 다읽었으면 추천할 수 없음
        if 0 not in predict_ratings:
            print("--------------------------------------------------------------------------------------------------------------")
            print(f"{'id':<5} {'title':<50} {'author':<30} {'avg.rating':<10} {'exp.rating':<10}")
            print("--------------------------------------------------------------------------------------------------------------")
            print("--------------------------------------------------------------------------------------------------------------")
            return
        
        # 임시 평점 계산
        for i in range(user_number):
            non_zero_ratings = user_item_matrix[i][user_item_matrix[i] != 0]
            mean = np.mean(non_zero_ratings) if len(non_zero_ratings) > 0 else 0
            for j in range(book_number):
                if user_item_matrix[i][j] == 0:
                    user_item_matrix[i][j] = mean
        
        # 사용자 간 코사인 유사도 계산
        user_index = user_list.index(user_id)
        cos_similarities = np.zeros(user_number)
        for i in range(user_number):
            if i != user_index:
                dot_product = np.dot(user_item_matrix[user_index], user_item_matrix[i])
                norm_product = np.linalg.norm(user_item_matrix[user_index]) * np.linalg.norm(user_item_matrix[i])
                cos_similarities[i] = dot_product / norm_product if norm_product != 0 else 0
        
        # 가중치 합을 이용한 평점 예측 및 추천 도서 결정
        best_score = -1.0
        b_id = -1
        for i in range(book_number):
            #i 번째 책의 예상 평점을 구하자.
            # 이미 읽은 책은 패스
            if predict_ratings[i] == -1:
                continue
            weighted_sum = 0
            sum_weight = 0
            for j in range(user_number):
                # 자기 자신은 제외
                if j == user_index:
                    continue
                weighted_sum += cos_similarities[j] * user_item_matrix[j][i]
                sum_weight += cos_similarities[j]
            predict_ratings[i] = weighted_sum / sum_weight if sum_weight != 0 else 0
            if predict_ratings[i] > best_score or (predict_ratings[i]==best_score and b_id > book_list[i]):
                b_id = book_list[i]
                best_score = predict_ratings[i]
        
        if b_id == -1 or best_score == -1: print("잘못 구현함")

        cursor.execute("""
            SELECT b.b_id, b_title, b_author, AVG(rating) 
            FROM books b
            LEFT OUTER JOIN borrowings r on b.b_id  = r.b_id
            WHERE b.b_id = %s 
            GROUP BY b.b_id
        """, (b_id,))
        book = cursor.fetchone()
        if book[3] == None : book[3] = 'None'
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"{'id':<5} {'title':<50} {'author':<30} {'avg.rating':<10} {'exp.rating':<10}")
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"{book[0]:<5} {book[1]:<50} {book[2]:<30} {book[3]:<10} {best_score:<10.4f}")
        print("--------------------------------------------------------------------------------------------------------------")
        

def main():
    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all books')
        print('3. print all users')
        print('4. insert a new book')
        print('5. remove a book')
        print('6. insert a new user')
        print('7. remove a user')
        print('8. check out a book')
        print('9. return and rate a book')
        print('10. print borrowing status of a user')
        print('11. search books')
        print('12. recommend a book for a user using popularity-based method')
        print('13. recommend a book for a user using user-based collaborative filtering')
        print('14. exit')
        print('15. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_books()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_book()
        elif menu == 5:
            remove_book()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            checkout_book()
        elif menu == 9:
            return_and_rate_book()
        elif menu == 10:
            print_borrowing_status_for_user()
        elif menu == 11:
            search_books()
        elif menu == 12:
            recommend_popularity()
        elif menu == 13:
            recommend_item_based()
        elif menu == 14:
            print('Bye!')
            break
        elif menu == 15:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()

connection.close()