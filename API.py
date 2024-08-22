from flask import Flask, request, jsonify

from CRUD import connect_db, read_records, read_no_pii, delete_record, update_record, create_record

app = Flask(__name__)


third_party_policy = {
    'Chase': ['credit_score', 'gender'],
    'BOA': ['city', 'name'],
    'Intuit': ['city', 'gender', 'credit_score', 'name']
}


# Initialize database and table
def init_db():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS people (
        name TEXT,
        city TEXT,
        ssn TEXT PRIMARY KEY,
        gender TEXT,
        credit_score INTEGER
    );
    ''')

    conn.commit()
    conn.close()


def check_name(input):
    if (len(input) <= 20) and " " in input:
        return True
    else:
        print("Name Failed")
        print(input)
        return False

def check_gender(input):
    if input == "M" or input == "F":
        return True
    else:
        print("Gender Failed")
        return False

def check_credit_score(input):
    if type(input) is int and 300 <= input <= 850:
        return True
    else:
        print("Credit score Failed")

        return False

def check_ssn(input):
    if 100000000 <= int(input) <= 999999999:
        return True
    else:
        print("SSN")

        return False


data_checker = {
    'gender': check_gender,
    'name': check_name,
    'ssn': check_ssn,
    'credit_score': check_credit_score
}


@app.route('/record', methods=['POST'])
def create_record_endpoint():
    data = request.json
    if check_name(data['name']) and check_gender(data['gender']) and check_credit_score(data['credit_score']) and check_ssn(data['ssn']):
        if not read_records(data['ssn']):
            create_record(data['name'], data['city'], data['ssn'], data['gender'], data['credit_score'])
            return jsonify({"message": "Record created successfully!"}), 201
    return jsonify({"message": "Record not created"}), 400


@app.route('/records', methods=['GET'])
def read_records_endpoint():
    ssn = request.args.get('ssn')
    records = read_records(ssn)
    return jsonify(records)


@app.route('/records/no_pii', methods=['GET'])
def read_no_pii_endpoint():
    input_column_list = request.args.get('input_column_list').split(',')
    filtered_ssn = request.args.get('ssn')
    policy_name = request.args.get('policy_name')
    records = read_no_pii(input_column_list, filtered_ssn, policy_name)
    return jsonify(records)



@app.route('/record', methods=['PUT'])
def update_record_endpoint():
    data = request.json
    if check_name(data['name']) and check_gender(data['gender']) and check_credit_score(data['credit_score']) and check_ssn(data['ssn']):
        if len(read_records(data['ssn'])) == 1:
            update_record(data['name'], data['city'], data['ssn'], data['gender'], data['credit_score'])
            return jsonify({"message": "Record updated successfully!"}), 201
    return jsonify({"message": "Record not updated"}), 400



@app.route('/record/<ssn>', methods=['DELETE'])
def delete_record_endpoint(ssn):
    delete_record(ssn)
    return jsonify({"message": "Record deleted successfully!"})


# Functions remain unchanged...

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=5001)
