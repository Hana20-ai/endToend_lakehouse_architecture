#!/bin/bash

# Specify the source and destination directories
source_directory="/bronze"
destination_directory="/bronze/sales"

# Prompt the user to enter the file names
echo "Enter file names (comma-separated): "
read file_names_input

# Split the input into an array of file names
IFS=',' read -ra file_names <<< "$file_names_input"

# Loop through the file names and copy them to the destination directory
for file_name in "${file_names[@]}"
do
    hdfs dfs -cp "${source_directory}/${file_name}" "${destination_directory}/"
done
