import sqlite3

db_name = 'data.db'

third_party_policy = {'Chase': ['credit_score','gender'], 'BOA': ['city', 'name'], 'Intuit': ['city', 'gender', 'credit_score', 'name']}

def connect_db():
    return sqlite3.connect(db_name)


# 1. Create (Insert)
def create_record(name, city, ssn, gender, credit_score):
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute('''
    INSERT INTO people (name, city, ssn, gender, credit_score)
    VALUES (?, ?, ?, ?, ?)
    ''', (name, city, ssn, gender, credit_score))

    conn.commit()
    conn.close()


# 2. Read (Select)
def read_records(filtered_ssn=None):
    conn = connect_db()
    cursor = conn.cursor()

    if filtered_ssn:
        cursor.execute('SELECT * FROM people WHERE ssn = ?', (filtered_ssn,))
    else:
        cursor.execute('SELECT * FROM people')

    records = cursor.fetchall()
    conn.close()

    return records


# 3. Read with no pii (Select)
def read_no_pii(input_column_list, filtered_ssn, policy_name):
    conn = connect_db()
    cursor = conn.cursor()

    policy = third_party_policy[policy_name]

    input_list_str = ""

    for n in input_column_list:
        if n in policy:
            input_list_str = input_list_str + ', ' + n
        else:
            print(f' no access to view {n}')
            return

    input_list_str = input_list_str[2:]
    print(input_list_str)
    cursor.execute(f'SELECT {input_list_str} FROM people WHERE ssn = {filtered_ssn}')

    records = cursor.fetchall()
    conn.close()

    return records


# 3. Update
def update_record(name, city, ssn, gender, credit_score):
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute('''
    UPDATE people
    SET name = ?, city = ?, gender = ?, credit_score = ?
    WHERE ssn = ?
    ''', (name, city, gender, credit_score, ssn))

    conn.commit()
    conn.close()


# 4. Delete
def delete_record(ssn):
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute('DELETE FROM people WHERE ssn = ?', (ssn,))

    conn.commit()
    conn.close()


# create_record('John Doe', 'Honolulu', '123456789', 'M', 750)
#
# delete_record('123456789')
#
# update_record('John Doe', 'San Francisco', '123456789', 'M', 800)
# lookup = str(input("please enter the social security number to look up "))
# record = read_records(lookup)
# if record:
#     print(record)
# else:
#     print(f"No records found for SSN: {lookup}.")