# Graph Analysis

## Overview
This project provides a PySpark application, a Python-based solution for analyzing graph data to find common neighbors between nodes in directed and undirected graphs. It is designed to handle large datasets using Apache Spark and can output results in various formats.

## Structure
The project directory is organized as follows:
- `src/`: Contains the core Python scripts.
  - `main.py`: The main script to run the analyses.
  - `utils.py`: Helper functions for data processing.
  - `logging_config.py`: Configures logging for the application.
- `tests/`: Unit tests for the application.
  - `test.py`: Tests for the utility functions.
- `logs/`: Directory for log files (created during runtime).
- `input/`: Contains a default directory for input.
  - `data/`: An example input - a directory that stores a CSV file.
    - `data.csv`: A CSV file to be read by the PySpark app.
- `output/`: Stores output files, typically CSVs, from analyses (created during runtime).
- `requirements.txt`: Specifies Python dependencies.
- `README.md`: Provides documentation.

## Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/LinoyBu96/Graph-Analysis.git
cd Graph-Analysis
pip install -r requirements.txt
```

## Usage

Before running the application, configure the input parameters in the command line as follows:

- `-n`: Number of top node pairs to retrieve (required).
- `--input`: Path to the CSV files containing the graph data (required).
- `--output_mode`: Output mode. Choose `show` to display on console, `save` to save to a CSV file, or `formatted_display` (default) to display on console in the following format:
  - `node1 = 101, node2 = 105, common = 2`
  - `node1 = 104, node2 = 107, common = 1`
  - `node1 = 101, node2 = 107, common = 1`
- `--output_path`: Path to save the results if the output mode is "save" (optional, will default to `./output/output_{current_time}.csv` if not provided).
- `--undirected`: Treat the graph as undirected (optional, add this flag to treat the graph as undirected).

## Running the Application

To run the application, use the `python3 ./src/main.py` command and the flags above from the root directory of your project. For example:

```bash
python3 ./src/main.py -n 3 --input input/data --output_mode show
```

## Output

- The results will be either shown on the console or saved to a CSV file in the `output` directory, depending on the chosen `--output_mode`.
- Logs will be generated in the configured logging directory.

## Additional Notes

- Ensure your Spark session is configured correctly in `src/utils.py` if modifications are needed.