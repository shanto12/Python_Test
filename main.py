import json
from pandas import DataFrame, to_numeric
import logging
from currency_converter import CurrencyConverter

c = CurrencyConverter()

CONFIG_FILE_PATH = "script_config.json"
LOG_FORMAT = '%(asctime)-15s %(message)s'


def read_config(config_path):
    """
    This function reads the configuration JSON file from the CONFIG_FILE_PATH location and converts that into a dictionary
    :param config_path: path of the
    :return: config_dict dictionary object
    """
    print(f"Reading config from path: {config_path}")
    try:
        with open(config_path, "r") as f:
            config_dict = json.load(f)
    except Exception as e:
        print(f"ERROR: Exception occurred while reading config from path: {config_path}")
        print(e)
    else:
        print(f"Successfully read config from path: {config_path}")
        return config_dict


def input_text_to_df(text, field_configuration):
    """
    This function converts the input text file to a Pandas DataFrame and returns it.
    :param text: input text file contents
    :param field_configuration: field configuratino dictionary
    :return: dataframe
    """
    value_table_dict = dict()
    for line in text.split("\n"):
        line_pos = 0
        for name, length in field_configuration.items():
            value_table_dict.setdefault(name, [])
            file_value = line[line_pos:line_pos + length].strip()
            # TODO Implement Field validation later.
            value_table_dict[name].append(file_value)
            line_pos += length

    return DataFrame.from_dict(value_table_dict)


def set_logger(python_log_level, log_file_path):
    """
    This function sets the log level, log file path and log format etc.
    :param python_log_level: This is read from the config file
    :param log_file_path: This is the path of the log file where the logs should be written to
    :return:
    """
    log_level = eval(f"logging.{python_log_level}") if python_log_level else logging.INFO
    log_file_path=log_file_path if log_file_path else "transactions.log"
    logging.basicConfig(level=log_level, filename=log_file_path, format=LOG_FORMAT)

    logging.info("*"*25 + "SCRIPT STARTED" + "*"*25)
    logging.info(f"Log Level set successfully and program started")


def convert_to_USD(currency_code, amount):
    """
    This is a future implementation where there are other currency codes if any.
    :param currency_code: Other currency code
    :param amount: amount of currency
    :return: currency value converted to USD.
    """
    # TODO test this function with other currencies.

    usd_value = c.convert(amount, currency_code.upper(), 'USD')
    return usd_value


def combine_column_values(row, column_list):
    """
    This function is used to create a new column by combining values from other columns.
    :param row: Each row passed to this function when executing a df.apply() command
    :param column_list: List of column names to combine and create the new value for the new column.
    :return: The value of the new column.
    """
    try:
        return "_".join(row[column_list])
    except:
        logging.error("exception occurred", exc_info=True)


def calculate_total_transaction(transactions_df, group_by_client_info_columns, group_by_product_info_columns):
    """
    This function calculates the total transaction per client, per product. The column names used to combine columns by is taken from the config file.
    :param transactions_df: Dataframe with transaction data
    :param group_by_client_info_columns: List of column names to group the clients by
    :param group_by_product_info_columns: List of column names to group the Products by
    :return:
    """
    try:
        transactions_df['quantity_long_sum'] = to_numeric(transactions_df['quantity_long'])
        transactions_df['quantity_short_sum'] = to_numeric(transactions_df['quantity_short'])

        transactions_df['client_info'] = transactions_df.apply(combine_column_values, axis=1,
                                                               column_list=group_by_client_info_columns)
        transactions_df['product_info'] = transactions_df.apply(combine_column_values, axis=1,
                                                                column_list=group_by_product_info_columns)

        grouped_df = transactions_df.groupby(['product_info', 'client_info']).agg(
            {'quantity_long_sum': 'sum', 'quantity_short_sum': 'sum'}).reset_index()

        grouped_df['total_transaction_amount'] = grouped_df['quantity_long_sum'] - grouped_df['quantity_short_sum']

        # TODO currency conversion to USD.
    except:
        logging.error("exception occurred", exc_info=True)
    else:
        logging.info(f"Successfully completed the calculate_total_transaction module. Returning the grouped_df")
        return grouped_df


def read_input_text(text_path):
    """
    This function reads a text file from the path passed to it and returns the string from it.
    :param text_path: Path to the text file
    :return: String contents of the text file.
    """
    try:
        with open(text_path, "r") as f:
            return f.read()
    except:
        logging.error("exception occurred", exc_info=True)

def df_rename(df):
    try:
        column_dict={col_name:col_name.replace("_", " ").title() for col_name in df.columns}
        df.rename(columns=column_dict, inplace=True)
    except:
        logging.error("exception occurred", exc_info=True)
    else:
        logging.info(f"Successfully converted the df column names to title letters.")

def df_to_csv(csv_path, df):
    """
    This function converts a DataFrame to a csv file.
    :param csv_path: Path to the csv file where the output needs to be created
    :param df: DataFrame object
    :return: None
    """
    try:
        df_rename(df)
        df.to_csv(csv_path, index=False)
    except:
        logging.error("exception occurred", exc_info=True)


def main():
    """
    This is the main function where the whole logic of the script is being controlled from.
    :return: None
    """
    # Read configuration from the config file.
    config_dict = read_config(CONFIG_FILE_PATH)
    set_logger(config_dict.get("python_log_level"), config_dict.get("log_file_path"))
    input_file_path = config_dict.get("input_file_path")
    if input_file_path:
        input_text = read_input_text(input_file_path)
        field_configuration = config_dict.get("field_configuration")
        if field_configuration:
            transactions_df = input_text_to_df(input_text, field_configuration)
            group_by_client_info_columns = config_dict.get("group_by_client_info_columns")
            group_by_product_info_columns = config_dict.get("group_by_client_info_columns")
            transaction_by_client_product_df = calculate_total_transaction(transactions_df,
                                                                           group_by_client_info_columns,
                                                                           group_by_product_info_columns)
            output_csv_path = config_dict.get("output_csv_path")
            df_to_csv(output_csv_path, transaction_by_client_product_df)

        else:
            logging.error(
                f"Couldn't read field_configuration from the config file: {CONFIG_FILE_PATH}. Hence not proceeding. Please check {CONFIG_FILE_PATH}")
            # TODO: write to logs
    else:
        logging.error(f"Couldn't read input_file_path. Hence not proceeding")
        # TODO: write to logs


if __name__ == '__main__':
    try:
        main()
    except:
        logging.error("exception occurred while calling the main module", exc_info=True)
    else:
        logging.info(f"Successfully completed the whole script. Please check the output file.")
        logging.info("*" * 25 + "SCRIPT ENDED" + "*" * 25)

