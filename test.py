import pandas as pd
import os

# Constants
CSV_DIRECTORY = "nepse price database"
SELECTED_COLUMNS = ["BUSINESS_DATE", "SECURITY_NAME", "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "CLOSE_PRICE",
                    "TOTAL_TRADED_QUANTITY", "TOTAL_TRADED_VALUE", "TOTAL_TRADES", "AVERAGE_TRADED_PRICE", "MARKET_CAPITALIZATION"]
OUTPUT_DIRECTORY = "csv_files"
PROCESSED_DATES_FILE = "processed_dates.txt"

# Create the output directory if it doesn't exist
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

def read_processed_dates():
    processed_dates = set()
    if os.path.exists(PROCESSED_DATES_FILE):
        with open(PROCESSED_DATES_FILE, "r") as f:
            processed_dates = set(f.read().splitlines())
    return processed_dates


def write_processed_dates(processed_dates):
    with open(PROCESSED_DATES_FILE, "w") as f:
        f.write("\n".join(processed_dates))


def process_csv_files():
    csv_files = sorted([file for file in os.listdir(CSV_DIRECTORY) if file.endswith(".csv")])

    processed_dates = read_processed_dates()

    for csv_file in csv_files:
        file_path = os.path.join(CSV_DIRECTORY, csv_file)
        data = pd.read_csv(file_path, on_bad_lines='skip')

        date_from_filename = csv_file.split("_")[1].split(".")[0]

        if date_from_filename not in processed_dates:
            for column_name in SELECTED_COLUMNS[2:]:
                column_data = data[["BUSINESS_DATE", "SECURITY_NAME", "SYMBOL", column_name]]
                column_filename = os.path.join(OUTPUT_DIRECTORY, f"{column_name.lower()}.csv")

                if os.path.exists(column_filename):
                    existing_data = pd.read_csv(column_filename)
                    updated_data = pd.concat([existing_data, column_data], ignore_index=True)
                    updated_data.drop_duplicates(inplace=True)
                    updated_data.sort_values(by=["SECURITY_NAME", "BUSINESS_DATE"], ascending=[True, False],
                                             inplace=True)
                    updated_data.to_csv(column_filename, index=False)
                    print(f"Data appended to existing CSV file '{column_filename}'.")
                else:
                    column_data.sort_values(by=["SECURITY_NAME", "BUSINESS_DATE"], ascending=[True, False],
                                            inplace=True)
                    column_data.to_csv(column_filename, index=False)
                    print(f"Individual CSV file '{column_filename}' created.")

            processed_dates.add(date_from_filename)

    write_processed_dates(processed_dates)
    print("All individual CSV files updated/created.")


if __name__ == "__main__":
    process_csv_files()
