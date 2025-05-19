import sys
from itertools import groupby
from operator import itemgetter

def mapper1(stream):
    # Phase 1 Mapper: Extract passenger ID and generate key-value pairs (passenger_id, 1)
    for line in stream:
        line = line.strip()
        if not line:
            continue
        passenger_id = line.split(',')[0]
        yield (passenger_id, 1)

def reducer1(key_value_stream):
    # Phase 1 Reducer: Count the number of flights for each passenger
    for key, group in groupby(key_value_stream, key=itemgetter(0)):
        total = sum(value for _, value in group)
        yield (key, total)

def mapper2(key_value_stream):
    # Phase 2 Mapper: Use a common key 'max' to aggregate all counts
    for key, value in key_value_stream:
        yield ('max', (value, key))

def reducer2(key_value_stream):
    # Phase 2 Reducer: Find the passenger(s) with the highest flight count
    max_count = -1
    max_passengers = []
    for key, group in key_value_stream:
        for _, (count, pid) in group:
            if count > max_count:
                max_count = count
                max_passengers = [pid]
            elif count == max_count:
                max_passengers.append(pid)
    return max_count, max_passengers

if __name__ == "__main__":
    # Execute the first phase of MapReduce
    with open('data/AComp_Passenger_data_no_error.csv', 'r') as f:
        # Mapper1 processes the input file
        mapped_data = list(mapper1(f))
        # Sort the data by key for the Reducer
        sorted_data = sorted(mapped_data, key=itemgetter(0))
        # Reducer1 aggregates the counts
        passenger_counts = list(reducer1(sorted_data))

    # Execute the second phase of MapReduce
    # Mapper2 processes the output from the first phase
    mapped_max = list(mapper2(passenger_counts))
    # Sort the data by key (all keys are 'max')
    sorted_max = sorted(mapped_max, key=itemgetter(0))
    # Group the data by key
    grouped_max = groupby(sorted_max, key=itemgetter(0))
    # Reducer2 finds the maximum count
    max_count, max_passengers = reducer2(grouped_max)

    # Output the result
    print("Passenger(s) with the highest number of flights:")
    for passenger in max_passengers:
        print(f"Passenger ID: {passenger}, Flights: {max_count}")