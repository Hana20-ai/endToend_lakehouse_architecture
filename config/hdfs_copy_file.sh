#!/bin/bash
# copy sales data into directory 
# Specify the source and destination directories
source_directory="/bronze"
destination_directory="/bronze/sales"

# Specify the list of file names to be copied
file_names=("sale_order" "product_product" "product_template" "product_marque" "dsd_category" "product_uom" "product_uom_categ" "res_company" "res_country" "res_country_state" "res_partner" "res_users" "res_partner_category" "dsd_region" "res_partner_title" "operating_unit" "res_store")
#"sale_order_line" "res_category" 
# Loop through the file names and copy them to the destination directory
for file_name in "${file_names[@]}"
do
    hdfs dfs -cp "${source_directory}/${file_name}" "${destination_directory}/"
done
