class SyntaxError :
    #문법오류
    pass


class CreateTableSuccess():
    #테이블 생성 성공
    def __init__(self, tableName):
        self.message = "'#tableName' table is created".replace("#tableName", tableName)

class DuplicateColumnDefError(Exception):
    #column 중복 정의
    def __init__(self):
        self.message = "Create table has failed: column definition is duplicated"

class DuplicatePrimaryKeyDefError(Exception):
    #primary key 중복 정의
    def __init__(self):
        self.message = "Create table has failed: primary key definition is duplicated"

class ReferenceTypeError(Exception):
    #reference type 오류
    def __init__(self):
        self.message = "Create table has failed: foreign key references wrong type"

class ReferenceArgError(Exception):
    #reference column 개수가 다를떄 오류
    def __init__(self):
        self.message = "Create table has failed: mismatch number foreign key columns"

class ReferenceNonPrimaryKeyError(Exception):
    #reference primary key 오류
    def __init__(self):
        self.message = "Create table has failed: foreign key references non primary key column"

class ReferenceColumnExistenceError(Exception):
    #reference column이 존재하지 않음
    def __init__(self):
        self.message = "Create table has failed: foreign key references non existing column"

class ReferenceTableExistenceError(Exception):
    #reference table이 존재하지 않음
    def __init__(self):
        self.message = "Create table has failed: foreign key references non existing table"

class NonExistingColumnDefError(Exception):
    #column이 존재하지 않음
    def __init__(self, column_name):
        self.message = "Create table has failed: '#colName' does not exist in column definition".replace("#colName", column_name) 

class TableExistenceError(Exception):
    #table이 이미 존재함
    def __init__(self):
        self.message = "Create table has failed: table with the same name already exists"

class CharLengthError(Exception):
    #char length 오류
    def __init__(self):
        self.message = "Char length should be over 0"

class DropSuccess():
    #테이블 삭제 성공
    def __init__(self, tableName):
        self.message = "'#tableName' table is dropped".replace("#tableName", tableName)

class NoSuchTable(Exception):
    #테이블이 존재하지 않음
    def __init__(self):
        self.message = "No such table"    

class DropReferencedTableError(Exception):
    #참조되는 테이블은 삭제할 수 없음
    def __init__(self, tableName):
        self.message = "Drop table has failed: '#tableName' is referenced by other table".replace("#tableName", tableName)
  
class InsertResult():
    #row 삽입 성공
    def __init__(self):
        self.message = "1 row inserted"

class SelectTableExistenceError(Exception):
    #테이블이 존재하지 않음
    def __init__(self, tableName):
        self.message = "Selection has failed: '#tableName' does not exist".replace("#tableName", tableName)

class DuplicatePrimaryKeyDefColumnError(Exception):
    # PK 정의에서 같은 칼럼이 여러번 입력됨.
    def __init__(self):
        self.message = "Create table has failed: primary key column is duplicated"
class InsertTypeMismatchError(Exception):
    #insert type 불일치
    def __init__(self):
        self.message = "Insertion has failed: Types are not matched"
class InsertColumnNonNullableError(Exception):
    #insert column이 nullable하지 않음
    def __init__(self, column_name):
        self.message = "Insertion has failed: '#colName' is not nullable".replace("#colName", column_name) 

class InsertColumnExistenceError(Exception):
    #insert column이 존재하지 않음
    def __init__(self, column_name):
        self.message = "Insertion has failed: '#colName' does not exist".replace("#colName", column_name)

class WhereIncomparableError(Exception):
    #where 타입 비교 불가
    def __init__(self):
        self.message = "Where clause trying to compare incomparable values"

class WhereColumnNotExistError(Exception):
    #where column이 존재하지 않음
    def __init__(self):
        self.message = "Where clause trying to reference non existing column"

class WhereTableNotSpecified(Exception):
    #where table이 명시되지 않음
    def __init__(self):
        self.message = "Where clause trying to reference tables which are not specified"
class DeleteResult() :
    #row 삭제 성공
    def __init__(self, count):
        self.message = "#count row(s) deleted".replace("#count", str(count))

class SelectColumnResolveError(Exception):
    def __init__(self, column_name):
        self.message = "Selection has failed: fail to resolve '#colName'".replace("#colName", column_name)

class WhereAmbiguousReference(Exception):
    
    def __init__(self):
        self.message = "Where clause contains ambiguous reference"

class InsertDuplicatePrimaryKeyError(Exception):
    def __init__(self):
        self.message = "Insertion has failed: Primary key duplication"