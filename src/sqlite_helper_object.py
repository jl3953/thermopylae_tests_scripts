import argparse
import sqlite3

import csv_utils


class SQLiteHelperObject:

  def __init__(self, db_file):
    self.db = db_file
    self.conn = None
    self.c = None

  def connect(self):
    self.conn = sqlite3.connect(self.db)
    self.c = self.conn.cursor()

  def create_table_if_not_exists(self, table_name, row_names_list):
    self.c.execute("CREATE TABLE IF NOT EXISTS {0} ({1})"
                   .format(table_name, ", ".join(row_names_list)))

  def insert_csv_data_into_sqlite_table(self, table_name, csv_fpath, *args, **kwargs):

    # read in csv file data
    header, data = csv_utils.read_in_data_as_tuples_float(csv_fpath)

    # create table if not exists yet
    column_names = SQLiteHelperObject.sanitize_column_names(header + list(kwargs.keys()))
    data_rows = [tuple(data_row + list(kwargs.values())) for data_row in data]
    question_marks = ",".join(["?"] * len(column_names))
    self.create_table_if_not_exists(table_name, column_names)

    # insert the rows
    insert_cmd = "INSERT INTO {0} VALUES ({1})".format(table_name, question_marks)
    self.c.executemany(insert_cmd, data_rows)
    self.conn.commit()

  def close(self):
    self.conn.close()

  @staticmethod
  def sanitize_column_names(column_names):
    def sanitize(col_name):
      col_name = col_name.replace("-", "_")
      col_name = col_name.replace("/", "_per_")
      col_name = col_name.replace("(", "_")
      col_name = col_name.replace(")", "")

      return col_name

    return [sanitize(cn) for cn in column_names]


def main():

  parser = argparse.ArgumentParser()
  parser.add_argument("db_file")
  parser.add_argument("csv")
  parser.add_argument("logs_dir")
  parser.add_argument("table_name")

  args = parser.parse_args()

  db = SQLiteHelperObject(args.db_file)
  db.connect()

  db.insert_csv_data_into_sqlite_table(args.table_name, args.csv,
                                       {"logs_dir": args.logs_dir})


if __name__ == "__main__":
  main()
