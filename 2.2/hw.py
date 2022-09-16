from sys import path_hooks
from delta.tables import *
import pyspark
from pyspark.sql import SparkSession
import codecs
import sys
import re
import os
import shutil
from datetime import datetime
import json


PID = os.getpid()


SYS = {}

try:
    config_file = codecs.open(sys.argv[1], "r", "utf_8_sig")
except OSError as _err:
    raise OSError("Unable to read file %s. " % sys.argv[1] + str(_err.args))


for _row in config_file:
    _row = _row.replace("\n", "").replace("\r", "")
    if not _row or _row[0] == "#":
        continue
    atr = re.search(r"#.*?=.*$|(.*)?=(.*)$", _row)
    if atr and atr.group(1):
        SYS[atr.group(1)] = atr.group(2).replace("\n", "").replace("\r", "")

config_file.close()

enable_log = int(SYS["EnableLog"])

LVL_LOG = {
    0: " ",
    1: "DEBUG",
    2: "INFORMATION",
    3: "WARNING",
    4: "CRITICAL",
}


def writelog(message, lvl):
    # function to create logs
    if 0 < enable_log <= lvl:
        log = (
            datetime.strftime(datetime.now(), "%d.%m.%Y	%H:%M:%S")
            + "\t"
            + LVL_LOG[lvl]
            + "\t"
            + message
            + "\n"
        )
        try:
            log_filename = SYS["LogFile"].replace(
                "YYYYMMDD", datetime.strftime(datetime.now(), "%Y%m%d")
            )
            write_log = open(log_filename, "a")
            write_log.write(log)
            write_log.close()
        except OSError:
            pass


def compelled(ms):
    writelog(ms, 4)
    finishing(1)


def finishing(state):
    writelog("Stop instance with PID(%s)\n" % PID, 2)
    try:
        os.unlink(SYS["PidFile"])
    except OSError as e:
        _, strerror = e.args
        writelog("Unable to delete file %s\n" % SYS["PidFile"] + strerror, 4)
    exit(state)


def check_pid(pid):
    # True if PID in subprocessors
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


if os.path.exists(SYS["PidFile"]):
    PID_READ = 0
    try:
        with open(SYS["PidFile"], "r") as PID_FILE:
            PID_READ = int(PID_FILE.readline())
        writelog("Read file %s" % SYS["PidFile"], 2)
    except IOError as _err:
        writelog("Can't read %s! " % SYS["PidFile"] + str(_err.args), 4)
        exit(1)
    except ValueError as _err:
        writelog("In %s incorrect pid value. " %
                 SYS["PidFile"] + str(_err.args), 4)
        exit(1)

    if PID_READ > 1 and check_pid(PID_READ):
        writelog("Another instance is already running", 4)
        exit(1)
    else:
        writelog("Process %s is undefined." % PID_READ, 3)
try:
    PID_FILE = open(SYS["PidFile"], "w")
    PID_FILE.write(str(PID))
    PID_FILE.close()
except OSError as _err:
    writelog("Can't create PID file! " + str(_err.args), 4)
    exit(1)


writelog("Start instance with PID(%s)." % PID, 2)
spark = SparkSession.builder.appName("delta_project") \
    .master("local") \
    .getOrCreate()

# изменим уровень логирвоания
spark.sparkContext.setLogLevel('WARN')

# Собираем запрос для ключей
keys = SYS['Keys'].split(',')
keys_query = ''

count = 1
for key in keys:
    keys_query += f"main.{key}=updates.{key}"
    if count != len(keys):
        keys_query += ' and '
    count += 1


# собираем запрос для обновления столбцов
columns = SYS['Columns'].split(',')

column_query = {}
count = 1

for column in columns:
    if column not in keys:
        column_query[column] = f"updates.{column}"

    count += 1

path_base = '/home/sag163/airflow/dags/project2.2/'


def load_data(path):
    # Загружаем зеркало и преобразуем в формат delta
    #
    try:
        if os.listdir(path_base + '/md_account_d') == []:

            main_table = spark.read.option("sep", ";").option("encoding", "UTF-8").option(
                'header', True).csv(path)

            main_table.repartition(1).write.csv(path=path_base +
                                                '/md_account_d', mode="overwrite", header="true", sep=';')

        else:

            df = spark.read.option("sep", ";").option(
                "encoding", "UTF-8").option('header', True).csv(path_base + '/md_account_d')

            delta_path = path_base + 'delta_table'
            os.mkdir(delta_path)

            df.write.format("delta").mode("overwrite").save(delta_path)

            main_table = DeltaTable.forPath(spark, delta_path)

            merge_table = spark.read.option("sep", ";").option("encoding", "UTF-8").option(
                'header', True).csv(path)

            main_table.alias("main") \
                .merge(merge_table.alias("updates"), keys_query) \
                .whenMatchedUpdate(set=column_query) \
                .whenNotMatchedInsertAll() \
                .execute()

            main_table.toDF().repartition(1).write.csv(path=path_base +
                                                       '/md_account_d', mode="overwrite", header="true", sep=';')
            print(f"Загружен {path}")
            main_table.toDF().show(50)
            shutil.rmtree(delta_path)
        writelog(f"Succsess load data for {path}", 2)

    except Exception as error:
        writelog(f"Error while load data for {path}: {error}", 4)


if __name__ == "__main__":
    try:
        delta_start = SYS['DeltaPath']

        deltas = sorted(os.listdir(delta_start))
        history_path = path_base + 'history.txt'
        history = {}
        if os.path.exists(history_path):
            string = codecs.open(history_path, "r", "utf_8_sig")
            history = json.load(string)

        for delt in deltas:
            # записываем номер
            if delt not in history.keys():
                files = os.listdir(delta_start+delt)
                history[delt] = datetime.strftime(
                    datetime.now(), "%d.%m.%Y	%H:%M:%S")
                for file in files:
                    if file.endswith('.csv'):

                        delta_path = delta_start+delt + '/' + file
                        load_data(delta_path)
            else:
                writelog(f"Delta with name {delt} already load", 2)

        config_file = codecs.open(history_path, "w", "utf_8_sig")
        data = json.dumps(history)

        config_file.write(data)
        finishing(1)
    except Exception as error:
        writelog(f"Error on main logic: {str()}")
