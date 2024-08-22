import pandas as pd
import random

df_last_name = pd.read_csv('./Resources/last-names.csv')

df_names = pd.read_csv('./Resources/all-names.csv')

df_cities = pd.read_csv('./Resources/us-cities-top-1k.csv')

for i in range(10):
    # assign weight to first name
    df_names['weight'] = df_names['n_sum'] / df_names['n_sum'].sum()

    # assign weight to last name
    df_last_name['weight'] = 1 / ((df_last_name.index + 1) / 20 + 1)

    # assign weight to city
    df_cities['weight'] = df_cities['Population'] / df_cities['Population'].sum()

    n = 10000000

    # generate first names
    generated_first_names = df_names.sample(n=n, replace=True, weights='weight')

    # generate last names
    generated_last_names = df_last_name.sample(n=n, replace=True, weights='weight')

    # generate cities
    generated_cities = df_cities.sample(n=n, replace=True, weights='weight')

    df_report = pd.DataFrame()

    generated_last_names.reset_index(drop=True, inplace=True)
    generated_first_names.reset_index(drop=True, inplace=True)
    generated_cities.reset_index(drop=True, inplace=True)

    # generate full names
    df_report['fullname'] = generated_first_names['name'] + " " + generated_last_names['Last Name']

    df_report['city'] = generated_cities['City']

    # generate 1 million social security numbers
    def generate_numbers(n):
        numbers = set()
        while len(numbers) < n:
            number = random.randint(100000000, 999999999)  # generate 8 digit number
            numbers.add(number)  # add number to the set
        return numbers


    # Generate 1 million unique numbers
    SSN = list(generate_numbers(n))

    df_report['SSN'] = SSN

    df_report['sex'] = generated_first_names['sex']

    # generate credit score based on normal distribution

    def generate_normal_numbers(mean, std_dev, lower_bound, upper_bound, count):
        numbers = []
        while len(numbers) < count:
            number = round(random.normalvariate(mean, std_dev))
            if lower_bound <= number <= upper_bound:
                numbers.append(number)
        return numbers

    mean_value = 625  # Mean value for the normal distribution
    std_dev_value = 100  # Standard deviation value for the normal distribution
    lower_bound_value = 400  # Lower bound of the range
    upper_bound_value = 850  # Upper bound of the range
    count_value = n  # Number of random numbers to generate

    df_report['credit_score'] = generate_normal_numbers(mean_value, std_dev_value, lower_bound_value, upper_bound_value, count_value)

    print(df_report.head())

    df_report.to_csv(f'./Resources/credit_report{i}.csv', index=False)
