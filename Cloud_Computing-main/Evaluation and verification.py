import csv
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

def analyze_passenger_data(file_path):
    # Process single CSV file to count flights per passenger.
    flight_counts = {}
    with open(file_path, mode='r', newline='') as f:
        csv_reader = csv.reader(f)
        for record in csv_reader:
            traveler_id = record[0]  # Extract passenger ID from first column
            # Increment flight count using dictionary's get method
            flight_counts[traveler_id] = flight_counts.get(traveler_id, 0) + 1
    return flight_counts

def merge_results(aggregate, partial):
    # Combine results from different file processors.
    for key, value in partial.items():
        aggregate[key] += value  # Accumulate counts across files
    return aggregate

def get_top_traveler(count_dict):
    # Identify passenger with maximum flight count.
    max_traveler = max(count_dict, key=lambda k: count_dict[k])
    return (max_traveler, count_dict[max_traveler])

def execute_processing():
    # Main data processing workflow.
    input_files = [
        'data/AComp_Passenger_data.csv',
        'data/AComp_Passenger_data_no_error_DateTime.csv'
    ]

    # Create process pool matching number of input files
    with ProcessPoolExecutor(max_workers=2) as executor:
        # Parallel execution across multiple files
        processed_results = list(executor.map(analyze_passenger_data, input_files))

    # Initialize aggregation container
    total_counts = defaultdict(int)
    
    # Merge results from all files
    for result in processed_results:
        merge_results(total_counts, result)

    # Generate per-file statistics
    for index, file_result in enumerate(processed_results):
        traveler_id, flight_num = get_top_traveler(file_result)
        print(f"File {input_files[index]}'s most frequent traveler: {traveler_id}, "
              f"Total flights: {flight_num}")

def main():
    # Program entry point.
    execute_processing()

if __name__ == '__main__':
    main()