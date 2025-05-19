# Cloud_Computing-main
Flight Passenger Analysis

Two Python scripts implementing different approaches to analyze passenger flight data and identify passengers with the highest number of flights.

Features

Script 1: MapReduce Implementation (`document1.py`)
• Two-phase MapReduce processing

• Phase 1: Count flights per passenger

• Phase 2: Find maximum flight records

• Designed for distributed processing of large datasets


Script 2: Parallel Processing Implementation (`document2.py`)
• Multi-file parallel processing

• CSV module for data reading

• Automatic result aggregation across files

• Per-file statistics reporting

• Memory-efficient counting with dictionaries


Technical Comparison

| Feature               | MapReduce Version       | Parallel Version         |
|-----------------------|-------------------------|--------------------------|
| Processing Architecture | Sequential            | Multi-process Parallel   |
| Data Handling          | Single-file processing | Multi-file parallel      |
| Max Count Algorithm   | Two-phase MapReduce    | In-memory dictionary     |
| Dependencies           | itertools/operator      | concurrent.futures       |
| Best For               | Large single files     | Multi-file datasets      |

Usage

Prerequisites
• Python 3.6+

• Standard libraries (no external dependencies)


Data Setup
1. Create data directory: `mkdir data`
2. Place data files:
   • `AComp_Passenger_data_no_error.csv`

   • `AComp_Passenger_data_no_error_DateTime.csv`


Execution
```bash
# Run MapReduce version
python document1.py

# Run parallel processing version
python document2.py
```

Sample Output

MapReduce Implementation
```
Passenger(s) with the highest number of flights:
Passenger ID: UES9151GS5, Flights: 25
```
S
Parallel Implementation
```
File data/AComp_Passenger_data.csv's most frequent traveler: UES9151GS5, Total flights: 24
File data/AComp_Passenger_data_no_error_DateTime.csv's most frequent traveler: UES9151GS5, Total flights: 25
```

Important Notes
1. Ensure correct file paths and naming
2. Data files must match code specifications
3. Use MapReduce version for very large datasets
4. Parallel version benefits from multi-core environments

License
MIT License

Author
[wanghaidong] - Technical support contact information

---

Customization Suggestions (Optional):
1. Add data format specifications
2. Include performance optimization tips
3. Document error handling approaches
4. Provide unit testing guidelines
5. Expand project roadmap

