import pandas as pd
import time
import hashlib

# convert SSN to list
df_report = pd.read_csv('./Resources/credit_report.csv')
ssn_list = df_report['SSN'].tolist()

ssn_input = [618972927, 758407341, 111111111]


def ssn_search_function(ssn_list, ssn_input):
    for ssn in ssn_input:
        if ssn in ssn_list:
            print(ssn)


ssn_set = set(ssn_list)


def hash_based_ssn_search(ssn_set, ssn_input):
    for ssn in ssn_input:
        if ssn in ssn_set:
            print(ssn)


start_time = time.time()

hash_based_ssn_search(ssn_set, ssn_input)

end_time = time.time()

elapsed_time = end_time - start_time

print("The function took", elapsed_time, "seconds to complete")

memory_list = [[] for _ in range(900)]


def hashing(inputs):
    for ssn in inputs:
        location = ssn // 1000000
        memory_list[location - 100].append(ssn)
    return memory_list


def custom_hash_search(ssn):
    location = ssn // 1000000
    if ssn in memory_list[location - 100]:
        return True
    return False


start_time = time.time()

for ssn in ssn_input:
    if custom_hash_search(ssn):
        print(ssn)

end_time = time.time()

elapsed_time = end_time - start_time

print("The function took", elapsed_time, "seconds to complete")

start_time = time.time()

for ssn in ssn_input:
    if ssn in ssn_list:
        print(ssn)

end_time = time.time()

elapsed_time = end_time - start_time

print("The function took", elapsed_time, "seconds to complete")

# convert name to list
df_report = pd.read_csv('./Resources/credit_report.csv')
name_list = df_report['fullname'].tolist()
memory_list2 = [[] for _ in range(1000)]

for name in name_list:
    index = hash(name) % 1000
    memory_list2[index].append(name)

name_input = ['Emma WELLS', 'No One', 'Jodi CRUZ']


def hash_based_name_search(n_list, names):
    for name in names:
        if name in n_list[hash(name) % 1000]:
            print(name)


hash_based_name_search(memory_list2, name_input)

name_list = [str(name) for name in name_list]
name_list.sort()


def binary_search(names, target):
    low = 0
    high = len(names) - 1

    while low <= high:
        mid = (low + high) // 2
        guess = str(names[mid])
        if guess == target:
            return mid  # Return the index of the target
        if guess > target:
            high = mid - 1
        else:
            low = mid + 1
    return None  # If the target is not in the list


print('=========')


for name in name_input:
    pos=binary_search(name_list, name)
    if pos is not None:
        print(name_list[pos])


