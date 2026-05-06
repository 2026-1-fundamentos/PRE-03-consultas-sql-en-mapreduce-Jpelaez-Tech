# pylint: disable=broad-exception-raised
# pylint: disable=import-error

# IMPORTANTE: Asegúrate de que el archivo se llame mapreduce.py (en minúsculas)
from .mapreduce import hadoop as run_mapreduce_job  # type: ignore

#
# Columns:
# total_bill, tip, sex, smoker, day, time, size
#

#
# SELECT *, tip/total_bill as tip_rate
# FROM tips;
#
def mapper_query_1(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip() + ",tip_rate"))
        else:
            row_values = row.strip().split(",")
            total_bill = float(row_values[0])
            tip = float(row_values[1])
            tip_rate = tip / total_bill
            result.append((index, row.strip() + "," + str(tip_rate)))
    return result


def reducer_query_1(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE time = 'Dinner';
#
def mapper_query_2(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner":
                result.append((index, row.strip()))
    return result


def reducer_query_2(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE time = 'Dinner' AND tip > 5.00;
#
def mapper_query_3(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner" and float(row_values[1]) > 5.00:
                result.append((index, row.strip()))
    return result


def reducer_query_3(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE size >= 5 OR total_bill > 45;
#
def mapper_query_4(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if int(row_values[6]) >= 5 or float(row_values[0]) > 45:
                result.append((index, row.strip()))
    return result


def reducer_query_4(sequence):
    """Reducer"""
    return sequence


#
# SELECT sex, count(*)
# FROM tips
# GROUP BY sex;
#
def mapper_query_5(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue
        row_values = row.strip().split(",")
        result.append((row_values[2], 1))
    return result


def reducer_query_5(sequence):
    """Reducer"""
    counter = dict()
    for key, value in sequence:
        if key not in counter:
            counter[key] = 0
        counter[key] += value
    return list(counter.items())


#
# SELECT day, AVG(tip)
# FROM tips
# GROUP BY day;
#
def mapper_query_6(sequence):
    """Mapper para promedio"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue
        row_values = row.strip().split(",")
        # day está en la posición 4, tip en la 1
        result.append((row_values[4], float(row_values[1])))
    return result


def reducer_query_6(sequence):
    """Reducer para promedio"""
    sums = {}
    counts = {}
    for key, value in sequence:
        if key not in sums:
            sums[key] = 0.0
            counts[key] = 0
        sums[key] += value
        counts[key] += 1
    
    # Calculamos el promedio para cada día
    return [(key, sums[key] / counts[key]) for key in sums]


#
# ORQUESTADOR:
#
def run():
    """Orquestador"""

    run_mapreduce_job(
        mapper_fn=mapper_query_1,
        reducer_fn=reducer_query_1,
        input_folder="files/input/",
        output_folder="files/query_1/",
    )

    run_mapreduce_job(
        mapper_fn=mapper_query_2,
        reducer_fn=reducer_query_2,
        input_folder="files/input/",
        output_folder="files/query_2/",
    )

    run_mapreduce_job(
        mapper_fn=mapper_query_3,
        reducer_fn=reducer_query_3,
        input_folder="files/input/",
        output_folder="files/query_3/",
    )

    run_mapreduce_job(
        mapper_fn=mapper_query_4,
        reducer_fn=reducer_query_4,
        input_folder="files/input/",
        output_folder="files/query_4/",
    )

    run_mapreduce_job(
        mapper_fn=mapper_query_5,
        reducer_fn=reducer_query_5,
        input_folder="files/input/",
        output_folder="files/query_5/",
    )

    run_mapreduce_job(
        mapper_fn=mapper_query_6,
        reducer_fn=reducer_query_6,
        input_folder="files/input/",
        output_folder="files/query_6/",
    )


if __name__ == "__main__":
    run()