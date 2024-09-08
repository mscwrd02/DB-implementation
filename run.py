from lark import Lark, Transformer
from lark.exceptions import UnexpectedInput
from berkeleydb import db
myDB = db.DB()
myDB.open('myDB.db', dbtype = db.DB_HASH, flags = db.DB_CREATE)

from my_json import *
from messages import *



def token_value_lower (token):
    return token.children[0].value.lower()
def token_value (token):
    return token.children[0].value
def token_type(token):
    return token.children[0].type

def find_table(table_name):
    return myDB.get(obj_to_ba("table: "+table_name))

def add_reference_map(table1, table2):
    # table1이 table2를 참조한다는 정보를 저장한다.
    reference_map = myDB.get(obj_to_ba("reference_map"))
    if reference_map == None:
        reference_map = []
    else : reference_map = ba_to_obj(reference_map)
    reference_map.append([table1, table2])
    myDB.put(obj_to_ba("reference_map"), obj_to_ba(reference_map))

def delete_reference_by(table_name):
    reference_map = myDB.get(obj_to_ba("reference_map"))
    if reference_map == None:
        return
    new_reference_map = []
    for reference in ba_to_obj(reference_map):
        if reference[0] != table_name:
            new_reference_map.append(reference)
    myDB.put(obj_to_ba("reference_map"), obj_to_ba(new_reference_map))

def get_reference_map():
    reference_map = myDB.get(obj_to_ba("reference_map"))
    if reference_map == None:
        return []
    else : return ba_to_obj(reference_map)

def insert_row(table_name, value):
    myDB.put(obj_to_ba({"table": table_name, "value":value}), obj_to_ba(value))
def delete_all_rows(table_name):
    cursor = myDB.cursor()
    while x := cursor.next():
        key = ba_to_obj(x[0])
        if  isinstance(key,str) : continue
        if key["table"] == table_name:
            myDB.delete(x[0])
    return
def retrieve_row(table_name):
    cursor = myDB.cursor()
    results = []
    while x := cursor.next():
        key = ba_to_obj(x[0])
        if  isinstance(key,str) : continue
        if key["table"] == table_name:
            results.append(ba_to_obj(x[1]))
    return results

def str_cmp(str1, str2,comp_op):
    test = None
    if comp_op == "=": test = True
    elif comp_op == "!=": test = False
    else : raise WhereIncomparableError()
    
    if len(str1) != len(str2) : return not test
    i = 1
    j = 1
    while i < len(str1)-1:
        if str1[i] != str2[j] : return not test
        i += 1
        j += 1
    return test
def int_cmp(int1, int2,comp_op):
    int1 = int(int1)
    int2 = int(int2)
    if comp_op == "=":
        return int1 == int2
    elif comp_op == "!=":
        return int1 != int2
    elif comp_op == ">":
        return int1 > int2
    elif comp_op == "<":
        return int1 < int2
    elif comp_op == ">=":
        return int1 >= int2
    elif comp_op == "<=":
        return int1 <= int2
    else : raise WhereIncomparableError()
def date_cmp(date1, date2,comp_op):
    
    #date를 int로 변환하여 비교한다.
    def date_val(date):
        date_val = 0
        for date_char in date:
            if date_char == '-': continue
            date_val = date_val*10 + int(date_char)
        return date_val
    date1_val = date_val(date1)
    date2_val = date_val(date2) 

    return int_cmp(date1_val,date2_val,comp_op)
    
def or_and_in_where(items):
    or_in_where = False
    and_in_where = False
    for be in items[0].find_data("boolean_expr"):
        
        if(len(be.children)>=3):
            if(be.children[1].value=="or"): or_in_where = True
    if not or_in_where:
        for bt in items[0].find_data("boolean_term"):
            if(len(bt.children)>=3):
                if(bt.children[1].value=="and"): and_in_where = True
    return or_in_where, and_in_where

class SqlParseTree(Transformer):
  
    def __init__(self):
        pass

    def create_table(self,items) :
        #table_name 추출
        for only_one_name in items[0].find_data("table_name"):
            table_name = token_value_lower(only_one_name)
        #table_name이 이미 존재확인
        if find_table(table_name) != None:
            raise TableExistenceError()

        #column 추출
        columns = []
        column_definitions= items[0].find_data("column_definition")
        for column_definition in column_definitions:
            #column_name 추출
            column_name = token_value_lower(column_definition.children[0])
            
            #column 중복 확인
            if column_name in [column["column_name"] for column in columns]:
                raise DuplicateColumnDefError()
            
            #column_type 추출
            column_type = token_value_lower(column_definition.children[1])
            
            if column_type.lower()=="char" : 
                if int(column_definition.children[1].children[2].value) < 1 : raise CharLengthError()
                column_type += column_definition.children[1].children[1] + column_definition.children[1].children[2] + column_definition.children[1].children[3]
            
            #nullable 추출
            if column_definition.children[2]==None:
                nullable = True
            else:
                nullable = False
            
            columns.append({
                "column_name" : column_name,
                "column_type" : column_type,
                "nullable" : nullable
            })
        
        #primary key 추출
        primary_keys = []
        primary_key_constraints = items[0].find_data("primary_key_constraint")
        is_primary_key_multiple = False
        for primary_key_constraint in primary_key_constraints:
            if(is_primary_key_multiple): raise DuplicatePrimaryKeyDefError()

            primary_key_column_names = primary_key_constraint.find_data("column_name")
            for primary_key_column_name in primary_key_column_names:
                if token_value_lower(primary_key_column_name) in primary_keys:
                    raise DuplicatePrimaryKeyDefColumnError()
                primary_keys.append(token_value_lower(primary_key_column_name))
            
            is_primary_key_multiple = True
        
        #primary key로 지정된 column이 column definition에 존재하는지 확인한다.
        for primary_key_column in primary_keys:
            if primary_key_column not in [column["column_name"] for column in columns]:
                raise NonExistingColumnDefError(primary_key_column)
        
        #primary key로 지정한 column을 nullable False로 지정한다.
        for column in columns:
            if column["column_name"] in primary_keys:
                column["nullable"] = False

        #foreign key 추출
        referential_constraints = items[0].find_data("referential_constraint")
        foreign_keys = []
        for referential_constraint in referential_constraints:
            reference_table_name = token_value_lower(referential_constraint.children[4])
            origin_column_names =[]
            for origin_column_name in referential_constraint.children[2].find_data("column_name"):
                #같은 칼럼이 여러번 입력된 경우 에러
                if origin_column_name in origin_column_names: raise ReferenceArgError()
                origin_column_names.append(token_value_lower(origin_column_name))
            
            reference_column_names = []
            for reference_column_name in referential_constraint.children[5].find_data("column_name"):
                #같은 칼럼이 여러번 입력된 경우 에러
                if reference_column_name in reference_column_names: raise ReferenceArgError()
                reference_column_names.append(token_value_lower(reference_column_name))
                
            #외래키 지정시 내 테이블 column에서 지정한 개수와 참조하는 테이블 column에서 지정한 개수가 같아야 한다.
            if len(origin_column_names) != len(reference_column_names): raise ReferenceArgError()
            # if already exist reference_table

            #이미 같은 테이블을 참조하는 foreign key가 있는 경우, 해당 foreign key에 추가한다.
            duplicated_table_reference = False
            for fk in foreign_keys:
                if fk["reference_table"] == reference_table_name:
                    duplicated_table_reference = True
                    #같은 테이블을 참조하는 FK를 추가할 때, 이미 입력된 칼럼이 이미 등록되어있는 경우 에러
                    if set(fk["columns"]) & set(origin_column_names) : raise ReferenceArgError()
                    fk["columns"] += origin_column_names
                    if set(fk["reference_columns"]) & set(reference_column_names) : raise ReferenceArgError()
                    fk["reference_columns"] += reference_column_names
                    break

            if not duplicated_table_reference:
                foreign_keys.append({
                    "columns" : origin_column_names,
                    "reference_table" : reference_table_name,
                    "reference_columns" : reference_column_names
                })  
        #foreign key로 지정한 column이 column definition에 존재하는지 확인한다.
        for foreign_key in foreign_keys:
            for column in foreign_key["columns"]:
                if column not in [column["column_name"] for column in columns]: raise NonExistingColumnDefError(column)

        #foreign key로 지정한 reference table이 존재하는지 확인한다.
        for foreign_key in foreign_keys:
            if find_table(foreign_key["reference_table"]) == None: raise ReferenceTableExistenceError()
        
        #foreign key로 지정한 reference column이 reference table에 존재하는지 확인한다.
        for foreign_key in foreign_keys:
            reference_table = ba_to_obj(find_table(foreign_key["reference_table"]))
            reference_table_columns = reference_table["columns"]
            reference_table_pks = reference_table["primary_keys"]
            mapped_columns = []
            for idx,want_to_reference_column in enumerate(foreign_key["reference_columns"]):
                
                #존재하는 칼럼을 참조하는지 확인
                mapped_column = None
                for ref_column in reference_table_columns:
                    if ref_column['column_name'] == want_to_reference_column:
                        mapped_column = ref_column['column_name']
                        break
                if mapped_column == None: raise ReferenceColumnExistenceError()
                mapped_columns.append([foreign_key["columns"][idx],mapped_column])
            #pk의 일부만 참조하는 경우 에러(pk가 없으면 아무거나 참조해도 됨)
            if(set(reference_table_pks) - set(foreign_key["reference_columns"]) != set()): raise ReferenceNonPrimaryKeyError()
            #참조하는 칼럼의 타입이 같은지 확인한다.
            for mapped_column in mapped_columns:
                origin_type = None
                reference_type = None
                for column in columns:
                    if column["column_name"] == mapped_column[0]:
                        origin_type = column["column_type"]
                for column in reference_table_columns:
                    if column["column_name"] == mapped_column[1]:
                        reference_type = column["column_type"]
                if origin_type != reference_type: raise ReferenceTypeError()

        
        myDB.put(obj_to_ba("table: "+table_name), obj_to_ba({"columns" : columns, "primary_keys" : primary_keys, "foreign_keys" : foreign_keys}))
        for foreign_key in foreign_keys:
            add_reference_map(table_name,foreign_key["reference_table"])
        
        return (CreateTableSuccess(table_name))

    def select_table(self, items):

        combined_table_data ={"columns":[]}
        combined_rows = []
        from_tables = []
        for idx, referred_table in enumerate(items[0].find_data("referred_table")):
            reference_table_name = token_value_lower(referred_table.children[0])
            reference_table = find_table(reference_table_name)
            if reference_table is None:
                raise SelectTableExistenceError(reference_table_name)
            from_tables.append(reference_table_name)
            reference_table_data = ba_to_obj(reference_table)
            for t_data in reference_table_data["columns"]:
                combined_table_data["columns"].append({"column_name" : reference_table_name + "." + t_data["column_name"], "column_type" : t_data["column_type"]})
            if idx == 0 :
                for original_row in retrieve_row(reference_table_name) :
                    combined_rows.append({reference_table_name+"."+key : value for key, value in original_row.items()})
            else :
                new_rows = []
                for new_row in retrieve_row(reference_table_name):
                    for original_row in combined_rows:
                        combined_row = {**original_row, **{reference_table_name+"."+key: value for key, value in new_row.items()}}
                        new_rows.append(combined_row)
                combined_rows = new_rows
        
        select_all = True
        print_columns = []

        for selected_column in items[0].find_data("selected_column"):
            select_all = False
            
            try :_table_name = token_value_lower(selected_column.children[0])
            except : _table_name = None
            _column_name = token_value_lower(selected_column.children[1])

            match_count = 0
            add_combine_column = None
            if _table_name == None :
                for combine_column in combined_table_data["columns"]:
                    if _column_name == combine_column["column_name"].split('.')[-1]:
                        match_count +=1
                        add_combine_column = {"column_name" : combine_column["column_name"], "column_type" : combine_column["column_type"]}
            else : 
                for combine_column in combined_table_data["columns"]:
                    if _table_name + "." + _column_name == combine_column["column_name"]:
                        match_count +=1
                        add_combine_column = {"column_name" : combine_column["column_name"], "column_type" : combine_column["column_type"]}
            #from절에 아예 없는 다른 테이블(존재하지 않는 경우도)이 사용된 경우에도 Resolve Error로 처리한다.
            if match_count == 0 or match_count > 1 : raise SelectColumnResolveError(_column_name)
            else : print_columns.append(add_combine_column)
        if select_all : print_columns = combined_table_data["columns"]

        where_clause_exist = False
        or_in_where, and_in_where = or_and_in_where(items)

        select_rows = [[],[]]
        for num_where, bf in enumerate(items[0].find_data("boolean_factor")):
            where_clause_exist = True
            not_in_where = False
            if(bf.children[0]!=None) : not_in_where = True
            for only_one_predicate in bf.find_data("predicate"):
                if(only_one_predicate.children[0].data == 'comparison_predicate'):
                    column_names = []
                    table_names = []
                    comparable_values = []
                    comp_op = None
                    #where 절에 사용된 칼럼, 값, 비교 연산자를 저장
                    for comp_operand in only_one_predicate.find_data("comp_operand"):
                        if len(comp_operand.children) > 1:
                            column_names.append(token_value_lower(comp_operand.children[1]))
                            if comp_operand.children[0] == None : table_names.append(None)
                            else : 
                                if token_value_lower(comp_operand.children[0]) not in from_tables : raise WhereTableNotSpecified()
                                table_names.append(token_value_lower(comp_operand.children[0]))
                        else : comparable_values.append([token_type(comp_operand.children[0]),token_value(comp_operand.children[0])])
                    for comp_op in only_one_predicate.find_data("comp_op"):
                        comp_op = token_value(comp_op)
                    
                    table_dot_columns = []
                    for idx, column_name in enumerate(column_names):

                        match_count = 0
                        add_table_dot_columns = None
                        if table_names[idx] == None :
                            for combine_column in combined_table_data["columns"]:
                                if column_name == combine_column["column_name"].split('.')[-1]:
                                    match_count +=1
                                    add_table_dot_columns = {"column_name" : combine_column["column_name"], "column_type" : combine_column["column_type"]}
                        else : 
                            for combine_column in combined_table_data["columns"]:
                                if table_names[idx] + "." + column_name == combine_column["column_name"]:
                                    match_count +=1
                                    add_table_dot_columns = {"column_name" : combine_column["column_name"], "column_type" : combine_column["column_type"]}
                        
                        if  match_count == 0: raise WhereColumnNotExistError()
                        if match_count > 1 : raise WhereAmbiguousReference()
                        table_dot_columns.append(add_table_dot_columns)

                    if len(table_dot_columns)==1 and len(comparable_values)==1 :
                        if "char"in table_dot_columns[0]["column_type"]:
                            if comparable_values[0][0] != "STR": raise WhereIncomparableError()
                            for combined_row in combined_rows:
                                if combined_row[table_dot_columns[0]["column_name"]]==None : continue
                                selected = str_cmp(combined_row[table_dot_columns[0]["column_name"]],comparable_values[0][1],comp_op)
                                if not_in_where : selected = not selected
                                if selected: select_rows[num_where].append(combined_row)
                        elif "int" == table_dot_columns[0]["column_type"]:
                            if comparable_values[0][0] != "INT": raise WhereIncomparableError()
                            for combined_row in combined_rows:
                                if combined_row[table_dot_columns[0]["column_name"]]==None : continue
                                selected = int_cmp(combined_row[table_dot_columns[0]["column_name"]],int(comparable_values[0][1]),comp_op)
                                
                                
                                if not_in_where : selected = not selected
                                if selected: select_rows[num_where].append(combined_row)
                        elif "date" == table_dot_columns[0]["column_type"]:
                            if comparable_values[0][0] != "DATE": raise WhereIncomparableError()
                            for combined_row in combined_rows:
                                if combined_row[table_dot_columns[0]["column_name"]]==None : continue
                                selected = date_cmp(combined_row[table_dot_columns[0]["column_name"]],comparable_values[0][1],comp_op)
                                if not_in_where : selected = not selected
                                if selected: select_rows[num_where].append(combined_row)
                    elif len(table_dot_columns) == 2 and len(comparable_values)==0:
                        if "char" in table_dot_columns[0]["column_type"] and "char" in table_dot_columns[1]["column_type"]:
                            for combined_row in combined_rows:
                                if combined_row[table_dot_columns[0]["column_name"]] ==None or combined_row[table_dot_columns[1]["column_name"]] ==None : continue
                                selected = str_cmp(combined_row[table_dot_columns[0]["column_name"]],combined_row[table_dot_columns[1]["column_name"]],comp_op)
                                if not_in_where : selected = not selected
                                if selected: select_rows[num_where].append(combined_row)
                        elif "int" == table_dot_columns[0]["column_type"] and "int" == table_dot_columns[1]["column_type"]:
                            for combined_row in combined_rows:
                                if combined_row[table_dot_columns[0]["column_name"]] ==None or combined_row[table_dot_columns[1]["column_name"]] ==None : continue
                                selected = int_cmp(combined_row[table_dot_columns[0]["column_name"]],combined_row[table_dot_columns[1]["column_name"]],comp_op)
                                if not_in_where : selected = not selected
                                if selected: select_rows[num_where].append(combined_row)
                        elif "date" == table_dot_columns[0]["column_type"] and "date" == table_dot_columns[1]["column_type"]:
                            for combined_row in combined_rows:
                                if combined_row[table_dot_columns[0]["column_name"]] ==None or combined_row[table_dot_columns[1]["column_name"]] ==None : continue
                                selected = date_cmp(combined_row[table_dot_columns[0]["column_name"]],combined_row[table_dot_columns[1]["column_name"]],comp_op)
                                if not_in_where : selected = not selected
                                if selected: select_rows[num_where].append(combined_row)
                        else : raise Exception("Unknown column type")
                    elif len(table_dot_columns) ==0 and len(comparable_values)==2:
                        selected = None
                        if comparable_values[0][0] != comparable_values[1][0]: raise WhereIncomparableError()
                        if comparable_values[0][0] == "STR":
                            selected = str_cmp(comparable_values[0][1], comparable_values[1][1], comp_op)
                        elif comparable_values[0][0] =="INT":
                            selected = int_cmp(int(comparable_values[0][1]), int(comparable_values[1][1]),comp_op)
                        elif comparable_values[0][0] =="DATE":
                            selected = date_cmp(comparable_values[0][1], comparable_values[1][1], comp_op)
                        if not_in_where : selected = not selected
                        if selected:
                            select_rows[num_where] = combined_rows
                    else : raise Exception("Unknown where clause")
                elif only_one_predicate.children[0].data == 'null_predicate':
                    column_name = None
                    table_name = None
                    for only_one_column_name in only_one_predicate.find_data("column_name"):
                        column_name = token_value_lower(only_one_column_name)
                    for only_one_table_name in only_one_predicate.find_data("table_name"):
                        table_name = token_value_lower(only_one_table_name)
             
                    match_count = 0
                    table_dot_column = None
                    for combine_column in combined_table_data["columns"]:
                        if (table_name==None and column_name == combine_column["column_name"].split('.')[-1]) or (table_name!=None and table_name + "."+ column_name == combine_column["column_name"]):
                            match_count += 1
                            table_dot_column = combine_column["column_name"]
                    
                    if match_count == 0: raise WhereColumnNotExistError()
                    if match_count > 1 : raise WhereAmbiguousReference()

                    select_null  = True
                    for only_one_null_operation in only_one_predicate.find_data("null_operation"):
                        if only_one_null_operation.children[1] == "not" : select_null = False
                    if not_in_where : select_null = not select_null
                    
                    for combined_row in combined_rows:
                        if(combined_row[table_dot_column]== None and select_null) or (combined_row[table_dot_column]!= None and not select_null):
                            select_rows[num_where].append(combined_row)
                else : raise Exception("Unknown where clause")
        
        select_targets = []

        if where_clause_exist:
            if or_in_where :
                select_targets = select_rows[0] + select_rows[1]
            elif and_in_where : 
                for first_where_select_row in select_rows[0]:
                    if first_where_select_row in select_rows[1] : select_targets.append(first_where_select_row)
            else : select_targets = select_rows[0]
        else : select_targets = combined_rows
        
        print_rows = []
        for select_target in select_targets:
            tmp = {}
            for print_column in print_columns:
                tmp[print_column["column_name"]] = select_target[print_column["column_name"]]
            if tmp in print_rows : continue
            print_rows.append(tmp)
        print_columns = {"columns" : print_columns}

        # table_data -> {"columns": [{"column_name" : "칼럼의 이름", "column_type"}]}
        # rows -> [{칼럼 이름에 해당되는 값의 dictionary}]

        column_widths = {column['column_name']: len(column['column_name']) for column in print_columns["columns"]}
        for row in print_rows:
            for column in print_columns["columns"]:
                column_widths[column['column_name']] = max(column_widths[column['column_name']], len(str(row[column['column_name']])))

        print("+" + "+".join(["-" * (column_widths[column['column_name']] + 2) for column in print_columns["columns"]]) + "+")
        for column in print_columns["columns"]:
            print(f"| {column['column_name']:{column_widths[column['column_name']]}} ", end="")
        print("|")
        print("+" + "+".join(["-" * (column_widths[column['column_name']] + 2) for column in print_columns["columns"]]) + "+")

        for row in print_rows:
            for column in print_columns["columns"]:
                print_str = str(row[column['column_name']]) if row[column['column_name']] != None else "NULL"
                if print_str!="NULL" and 'char' in column['column_type'] : 
                    print_str = print_str[1:-1]
                print(f"| {print_str:{column_widths[column['column_name']]}} ", end="")
            print("|")

        print("+" + "+".join(["-" * (column_widths[column['column_name']] + 2) for column in print_columns["columns"]]) + "+")


        return 
    def drop_table(self,items) :
        table_name = None
        for only_one_name in items[0].find_data("table_name"):
            table_name = token_value_lower(only_one_name)
        if find_table(table_name) == None:
            raise NoSuchTable()
        reference_map = get_reference_map()
        
        #현재 테이블을 다른 테이블이 참조하는 경우 삭제를 거부한다.
        for reference in reference_map:
            if reference[1]==table_name:
                raise DropReferencedTableError(table_name)
        
        myDB.delete(obj_to_ba("table: "+table_name))

        #다른 테이블을 참고하고 있던 경우, 참조 정보를 삭제한다.
        delete_reference_by(table_name)
        delete_all_rows(table_name)

        return DropSuccess(table_name)
    def explain_table(self,items) :
        table_name = None
        for only_one_name in items[0].find_data("table_name"):
            table_name = token_value_lower(only_one_name)
        
        table = find_table(table_name)
        if table == None:
            raise NoSuchTable()
        
        table_data = ba_to_obj(table)
        print("-----------------------------------------------------------------")
        print(f"table_name [{table_name}]")
        print(f"{'column_name':<20} {'type':<10} {'null':<10} {'key':<10}")
        for column in table_data["columns"]:
            is_foreign_key = any(column["column_name"] in fk["columns"] for fk in table_data["foreign_keys"])
            column_name = column["column_name"]
            column_type = column["column_type"]
            if column["nullable"] : 
                nullable = "Y"
            else : nullable = "N"
            if column_name in table_data["primary_keys"] and is_foreign_key:
                key = "PRI/FOR"
            elif column_name in table_data["primary_keys"]:
                key = "PRI"
            elif is_foreign_key:
                key = "FOR"
            else: key = ""
            print(f"{column_name:<20} {column_type:<10} {nullable:<10} {key:<10}")
        print("-----------------------------------------------------------------")

        return 
    def describe_table(self,items) :
        return self.explain_table(items)
    def desc_table(self,items) :
        return self.explain_table(items)
    def insert_table(self,items) :
        table_name = None
        for only_one_name in items[0].find_data("table_name"):
            table_name = token_value_lower(only_one_name)
        table_info = find_table(table_name)
        if table_info == None:
            raise NoSuchTable()
        table_info = ba_to_obj(table_info)

        column_names =[]
        for column_name in items[0].find_data("column_name"):
            name = token_value_lower(column_name)
            #입력으로 들어온 column_name이 table에 존재하지 않으면 에러
            if name not in [column["column_name"] for column in table_info["columns"]]:
                raise InsertColumnExistenceError(name)
            column_names.append(name)        
        if(len(column_names)==0):
            column_names = [column["column_name"] for column in table_info["columns"]]

        values = []
        for insert_value in items[0].find_data("insert_value"):
            try :
                if token_value(insert_value) == 'NULL': values.append({"type": "NULL", "value" : None})
            except : values.append({"type" : token_type(insert_value.children[0]), "value":(token_value(insert_value.children[0]))})
        #column_names의 lengh, values의 length, table_info["columns"]의 length가 다르면 에러
        if(len(column_names) != len(values)):
            raise InsertTypeMismatchError()
        
        
        value = {}
        for column in table_info["columns"]:
            try:
                idx = column_names.index(column["column_name"])
            except:
                #table_info에 존재하는데, column_name이 입력되지 않았을때 -> null을 삽입해야함
                if(column["nullable"] == False):
                    raise InsertColumnNonNullableError(column["column_name"])
                else :
                    value[column["column_name"]] = None
                    continue
            # 입력값으로 null이 들어온 경우
            if values[idx]["value"] == None :
                if(column["nullable"] == False):
                    raise InsertColumnNonNullableError(column["column_name"])
                else :
                    value[column["column_name"]] = None
                    continue
            
            #char의 경우, 길이를 초과하는 경우 잘라낸다.
            col_type = column["column_type"]
            col_name = column["column_name"]
            if 'char' in col_type:
                if values[idx]["type"] != "STR" : raise InsertTypeMismatchError()
                start_index = col_type.find('(') + 1
                end_index = col_type.find(')')
                size = int(col_type[start_index:end_index])
                if len(values[idx]["value"]) > size+2:
                    values[idx]["value"] = values[idx]["value"][0] + values[idx]["value"][1:size+1] + values[idx]["value"][-1]
            value[col_name] = values[idx]["value"]
            if col_type =="int":
                if values[idx]["type"]!="INT" : raise InsertTypeMismatchError()
            if col_type =="date":
                if values[idx]["type"]!="DATE" : raise InsertTypeMismatchError()
        

        #primary key가 존재하는 경우, 중복된 key가 있는지 확인한다.
        if len(table_info["primary_keys"]) > 0:
            primary_key_values = []
            for primary_key in table_info["primary_keys"]:
                primary_key_values.append(value[primary_key])
            cursor = myDB.cursor()
            while x := cursor.next():
                key = ba_to_obj(x[0])
                if  isinstance(key,str) : continue
                if key["table"] == table_name:
                    row = ba_to_obj(x[1])
                    if [row[primary_key] for primary_key in table_info["primary_keys"]] == primary_key_values:
                        raise InsertDuplicatePrimaryKeyError()
        insert_row(table_name, value)
        
        return InsertResult()
    def delete_table(self,items) :
        table_name = None
        for only_one_name in items[0].find_data("table_name"):
            table_name = token_value_lower(only_one_name)
        table_info = find_table(table_name)
        if table_info == None:
            raise NoSuchTable()
        table_info = ba_to_obj(table_info)
        where_clause_exist = False
        or_in_where, and_in_where = or_and_in_where(items)
        
        delete_keys = [[],[]]
        for num_where, bf in enumerate(items[0].find_data("boolean_factor")):
            where_clause_exist = True
            not_in_where = False
            if(bf.children[0]!=None) : not_in_where = True
            for only_one_predicate in bf.find_data("predicate"):

                #where 절에 사용된 칼럼이 from 절에 사용된 table_name과 다르면 에러
                for where_table_name in only_one_predicate.find_data("table_name"):
                    if token_value_lower(where_table_name) != table_name: raise WhereTableNotSpecified()
                
                if(only_one_predicate.children[0].data == 'comparison_predicate'):
                    column_names = []
                    comparable_values = []
                    comp_op = None
                    #where 절에 사용된 칼럼, 값, 비교 연산자를 저장
                    for column_name in only_one_predicate.find_data("column_name"):
                        column_names.append(token_value_lower(column_name))
                    for comparable_value in only_one_predicate.find_data("comparable_value"):
                        comparable_values.append([token_type(comparable_value),token_value(comparable_value)])
                    for comp_op in only_one_predicate.find_data("comp_op"):
                        comp_op = token_value(comp_op)
                    

                    #where 절에서 column과 comparable_value를 비교하는 케이스
                    if len(column_names)==1 and len(comparable_values)==1 :
                        matched = False
                        for table_columns in table_info["columns"]:
                            #table_info에 존재하는 column확인
                            if(table_columns["column_name"] == column_names[0]):
                                matched = True
                                if "char" in table_columns["column_type"]:
                                    #comparable_value의 타입이 str이 아니면 에러
                                    if comparable_values[0][0] != "STR":
                                        raise WhereIncomparableError()
                                    cursor = myDB.cursor()
                                    while x := cursor.next():
                                        key = ba_to_obj(x[0])
                                        if  isinstance(key,str) : continue
                                        if key["table"] == table_name:
                                            row = ba_to_obj(x[1])
                                            selected = str_cmp(row[column_names[0]],comparable_values[0][1],comp_op)                                            
                                            if not_in_where : selected = not selected
                                            if selected: delete_keys[num_where].append(x[0])
                                elif "int" == table_columns["column_type"]:
                                    #comparable_value의 타입이 int가 아니면 에러
                                    if comparable_values[0][0] != "INT": raise WhereIncomparableError()

                                    cursor = myDB.cursor()
                                    while x := cursor.next():
                                        key = ba_to_obj(x[0])
                                        if  isinstance(key,str) : continue
                                        if key["table"] == table_name:
                                            row = ba_to_obj(x[1])
                                            selected = int_cmp(int(row[column_names[0]]),int(comparable_values[0][1]),comp_op)
                                            if not_in_where : selected = not selected
                                            if selected: delete_keys[num_where].append(x[0])
                                elif "date" == table_columns["column_type"]:
                                    if comparable_values[0][0] != "DATE": raise WhereIncomparableError()
                                    cursor = myDB.cursor()
                                    while x := cursor.next():
                                        key = ba_to_obj(x[0])
                                        if  isinstance(key,str) : continue
                                        if key["table"] == table_name:
                                            row = ba_to_obj(x[1])
                                            selected = date_cmp(row[column_names[0]],comparable_values[0][1],comp_op)
                                            if not_in_where : selected = not selected
                                            if selected: delete_keys[num_where].append(x[0])
                                else : raise Exception("Unknown column type")
                        if not matched : raise WhereColumnNotExistError()
                    elif len(column_names) == 2 and len(comparable_values)==0:
                        #table 정보에서, where 절에 쓰인 칼럼 추출하기
                        col1 = None
                        col2  = None
                        for _column in table_info["columns"] :
                            if _column["column_name"]==column_names[0] : col1 = _column
                            if _column["column_name"]==column_names[1] : col2 = _column
                        if col1 == None or col2 == None : raise WhereColumnNotExistError()

                        if "char" in col1["column_type"] and "char" in col2["column_type"]:
                            cursor = myDB.cursor()
                            while x := cursor.next():
                                key = ba_to_obj(x[0])
                                if  isinstance(key,str) : continue
                                if key["table"] == table_name:
                                    row = ba_to_obj(x[1])
                                    selected = str_cmp(row[column_names[0]],row[column_names[1]],comp_op)                                            
                                    if not_in_where : selected = not selected
                                    if selected: delete_keys[num_where].append(x[0])
                        elif "int" == col1["column_type"] and "int" == col2["column_type"]:
                            cursor = myDB.cursor()
                            while x := cursor.next():
                                key = ba_to_obj(x[0])
                                if  isinstance(key,str) : continue
                                if key["table"] == table_name:
                                    row = ba_to_obj(x[1])
                                    selected = int_cmp(int(row[column_names[0]]),int(row[column_names[1]]),comp_op)
                                    if not_in_where : selected = not selected
                                    if selected: delete_keys[num_where].append(x[0])
                        elif "date" == col1["column_type"] and "date" == col2["column_type"]:
                            cursor = myDB.cursor()
                            while x := cursor.next():
                                key = ba_to_obj(x[0])
                                if  isinstance(key,str) : continue
                                if key["table"] == table_name:
                                    row = ba_to_obj(x[1])
                                    selected = date_cmp(row[column_names[0]],row[column_names[1]],comp_op)
                                    if not_in_where : selected = not selected
                                    if selected: delete_keys[num_where].append(x[0])
                        else : raise WhereIncomparableError()
                    elif len(column_names) ==0 and len(comparable_values) ==2:
                        selected = None
                        if comparable_values[0][0] != comparable_values[1][0]: raise WhereIncomparableError()
                        if comparable_values[0][0] == "STR":
                            selected = str_cmp(comparable_values[0][1], comparable_values[1][1], comp_op)
                        elif comparable_values[0][0] =="INT":
                            selected = int_cmp(int(comparable_values[0][1]), int(comparable_values[1][1]),comp_op)
                        elif comparable_values[0][0] =="DATE":
                            selected = date_cmp(comparable_values[0][1], comparable_values[1][1], comp_op)
                        if not_in_where : selected = not selected
                        if selected : 
                            cursor = myDB.cursor()
                            while x := cursor.next():
                                key = ba_to_obj(x[0])
                                if  isinstance(key,str) : continue
                                if key["table"] == table_name: delete_keys[num_where].append(x[0])
                    else : raise Exception("UnexpectedInput")
                elif (only_one_predicate.children[0].data == 'null_predicate'):
                    #column_name 뽑고
                    #null_operation의 자식 3개 -> is not null 에서 두번째만 not 인지 none인지 검사하면 될듯
                    column_name = None
                    for only_one_column_name in only_one_predicate.find_data("column_name"):
                        column_name = token_value_lower(only_one_column_name)
                    select_null  = True

                    #is null 인지, is not null인지 검사하여 select_null 변경
                    for only_one_null_operation in only_one_predicate.find_data("null_operation"):
                        if only_one_null_operation.children[1] == "not" : select_null = False
                    
                    cursor = myDB.cursor()
                    while x := cursor.next():
                        key = ba_to_obj(x[0])
                        if  isinstance(key,str) : continue
                        if key["table"] == table_name:
                            row = ba_to_obj(x[1])
                            selected = False
                            if (row[column_name] ==None and select_null == True) or (row[column_name] !=None and select_null == False) : selected = True                                          
                            if not_in_where : selected = not selected
                            if selected: delete_keys[num_where].append(x[0])

                else : raise Exception("UnexpectedInput")

        delete_targets = None
        if where_clause_exist:
            if or_in_where :
                delete_targets = set(delete_keys[0]).union(set(delete_keys[1]))
            elif and_in_where : 
                delete_targets = set(delete_keys[0]).intersection(set(delete_keys[1]))
            else : delete_targets = set(delete_keys[0])
        #where 절이 없으면 모든 row를 지운다
        else :
            delete_targets = set()
            cursor = myDB.cursor()
            while x := cursor.next():
                key = ba_to_obj(x[0])
                if  isinstance(key,str) : continue
                if key["table"] == table_name:
                    delete_targets.add(x[0])
        for delete_target in delete_targets:
                cursor = myDB.cursor()
                while x := cursor.next():
                    if  x[0] == delete_target:
                        myDB.delete(x[0])

        return DeleteResult(len(delete_targets))
    def show_tables(self,items) :
        cursor = myDB.cursor()
        print("-----------------------------------------------------------------")
        while x := cursor.next():
            if not isinstance(ba_to_obj(x[0]),str) : continue
            if ba_to_obj(x[0])[:5] != "table": continue
            print(ba_to_obj(x[0])[6:])
        print("-----------------------------------------------------------------")
        return
    def update_table(self,items) :
        print("DB_2020-12696> 'UPDATE' requested")
        return items[0]
    def exit(self,items) :
        return "EXIT"
        
#같은 디렉토리의 grammar.lark을 로드하고, lalr을 이용해 파싱한다. 
parser = Lark.open("grammar.lark", rel_to=__file__, start='command',parser='lalr',transformer=SqlParseTree())
def main():
    while True:
        try:
            lines = []
            print('DB_2020-12696> ', end='')
            while True:
                line = input()
                lines.append(line)
                if ';' in line:
                    break
            #여러줄의 입력을 받을 경우, 띄어쓰기로 이어붙인 하나의 문자열로 만들어 파싱한다.
            command = ' '.join(lines)
            output = parser.parse(command)
            
            if output == 'EXIT':
                break
            result = output.children[0].children[0]
            if isinstance(result, CreateTableSuccess):                
                print("DB_2020-12696>", result.message)
            elif isinstance(result, DropSuccess):
                print("DB_2020-12696>", result.message)
            elif isinstance(result, InsertResult):
                print("DB_2020-12696>",result.message)
            elif isinstance(result, DeleteResult):
                print("DB_2020-12696>",result.message)
            #print(output.pretty())
        except UnexpectedInput:
            print("DB_2020-12696> Syntax error")
        except Exception as e:
            print("DB_2020-12696>", e.message)
main()
