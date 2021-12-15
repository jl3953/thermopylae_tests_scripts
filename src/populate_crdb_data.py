import csv
import sys


def populate(filename, range_max, range_min=0):
    with open(filename, "w") as f:
        writer = csv.writer(f)

        for i in range(range_min, range_max+1):
            writer.writerow((i, i))


def main():
    filename = "/home/jennifer/thermopylae_tests/scratch/populate.csv"
    range_max = 1000000
    populate(filename, range_max)

    return 0


if __name__ == "__main__":
    sys.exit(main())
