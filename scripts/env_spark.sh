#!/usr/bin/env bash
# Configura o ambiente para rodar PySpark + Delta Lake localmente no Windows.
# Uso: source scripts/env_spark.sh
export JAVA_HOME="/c/Program Files/Microsoft/jdk-17.0.19.10-hotspot"
export HADOOP_HOME="/c/hadoop"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
export SPARK_LOCAL_IP="127.0.0.1"
export PYSPARK_PYTHON="/c/Users/Rinaldo/Data_Eng/.venv/Scripts/python.exe"
export PYSPARK_DRIVER_PYTHON="/c/Users/Rinaldo/Data_Eng/.venv/Scripts/python.exe"
export PYTHON_VENV="/c/Users/Rinaldo/Data_Eng/.venv/Scripts/python.exe"
