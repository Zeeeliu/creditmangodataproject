import psycopg2

# Update these details according to your PostgreSQL setup
db_name = 'credit_mango'
user = 'postgres'
password = 'testtest'
host = '0.0.0.0'
port = '5432'

third_party_policy = {'Chase': ['credit_score', 'gender'], 'BOA': ['city', 'name'], 'Intuit': ['city', 'gender', 'credit_score', 'name']}


def connect_db():
    return psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port
    )


def create_record(name, city, ssn, gender, credit_score):
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute('''
    INSERT INTO people (name, city, ssn, gender, credit_score)
    VALUES (%s, %s, %s, %s, %s)
    ''', (name, city, ssn, gender, credit_score))

    conn.commit()
    conn.close()


def read_records(filtered_ssn=None):
    conn = connect_db()
    cursor = conn.cursor()

    if filtered_ssn:
        cursor.execute('SELECT * FROM people WHERE ssn = %s', (filtered_ssn,))
    else:
        cursor.execute('SELECT * FROM people')

    records = cursor.fetchall()
    conn.close()

    return records


def read_no_pii(input_column_list, filtered_ssn, policy_name):
    conn = connect_db()
    cursor = conn.cursor()

    policy = third_party_policy[policy_name]

    input_list_str = ""

    for n in input_column_list:
        if n in policy:
            input_list_str += ', ' + n
        else:
            print(f'No access to view {n}')
            return

    input_list_str = input_list_str[2:]

    cursor.execute(f'SELECT {input_list_str} FROM people WHERE ssn = %s', (filtered_ssn,))

    records = cursor.fetchall()
    conn.close()

    return records


def update_record(name, city, ssn, gender, credit_score):
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute('''
    UPDATE people
    SET name = %s, city = %s, gender = %s, credit_score = %s
    WHERE ssn = %s
    ''', (name, city, gender, credit_score, ssn))

    conn.commit()
    conn.close()


def delete_record(ssn):
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute('DELETE FROM people WHERE ssn = %s', (ssn,))

    conn.commit()
    conn.close()
