# COT Data Automation

This project automates the process of data extraction from the Commodity Futures Trading Commission (CFTC) website, processing, cleaning, and pushing it to a backend database. The project uses the [cot_reports](https://github.com/NDelventhal/cot_reports) module for web scraping.

## Setup

- 1. Clone the repository:

    ```
    git clone https://github.com/faisalanjum/Cot-Data-Automation.git
    ```
   
- 2. Install the dependencies:


- 3. Create a `.env` file in the root directory and add the following environment variables:

 ```
 DB_URL_COT = postgresql://username:password@localhost:5432/cot
 POSTGRES_SCHEMA_COT = schema_name
 ```

Replace `username`, `password`, and `schema_name` with the appropriate values for your PostgreSQL database.

- 4. Create the necessary tables in the database:

 ```
 python backend/db/migrate_db.py
 ```

## Usage

To run the script, execute the following command in the terminal:
 py -m cot.py
 
 This will extract the data from the CFTC website, process and clean it, and insert it into the database.

## Directory Structure

The project contains the following directories:

- `backend`: Contains the database definition, structure, migration, updation, and retrieval scripts.
    - `controller`: Contains the preprocessing and formatting code.
    - `db`: Contains the database definition, structure, migration, updation, and retrieval scripts.

## Contributing

Contributions to the project are always welcome! To contribute, please create a pull request and explain the changes you have made.


