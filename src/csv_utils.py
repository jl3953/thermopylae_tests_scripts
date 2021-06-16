import csv
import os


def read_in_data(filename):
  data = []
  if os.path.exists(filename):
    with open(filename, "r") as csvfile:
      reader = csv.DictReader(csvfile, delimiter='\t')
      for row in reader:
        for key in row:
          row[key] = float(row[key])

        data.append(dict(row))

  return data


def read_in_data_as_tuples_float(csv_fpath):
  header, tuples = read_in_data_as_tuples(csv_fpath)

  converted_tuples = []
  for tuple in tuples:
    converted_tuples.append([float(val) for val in tuple])

  return header, converted_tuples


def read_in_data_as_tuples(csv_fpath, has_header=True):
  header = None
  tuples = []

  with open(csv_fpath, "r") as f:
    reader = csv.reader(f, delimiter='\t')

    is_first_row = has_header
    for row in reader:
      if is_first_row:
        header = row
        is_first_row = False
      else:
        tuples.append(row)

  return header, tuples


def write_out_data(dict_list, filename, has_header=True):
  return write_out_data_helper(dict_list, filename, mode="w", has_header=has_header)


def append_data_to_file(dict_list, filename):
  return write_out_data_helper(dict_list, filename, mode="a")


def write_out_data_helper(dict_list, filename, mode="w", has_header=True):
  if len(dict_list) <= 0:
    return ""

  with open(filename, mode) as csvfile:
    writer = csv.DictWriter(csvfile, delimiter='\t', fieldnames=dict_list[0].keys())

    if mode == "w" and has_header:
      writer.writeheader()

    for datum in dict_list:
      try:
        writer.writerow(datum)
      except BaseException:
        print("failed on {0}".format(datum))
        continue

  return filename
