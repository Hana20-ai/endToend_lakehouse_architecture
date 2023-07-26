from delta.tables import DeltaTable
from pyspark.sql.functions import lit

#Step 1: Load Delta tables into DataFrames
sale_order_df = spark.read.format("delta").load("/delta/silver/sale_order")
sale_order_line_df = spark.read.format("delta").load("/delta/silver/sale_order_line")
product_product_df = spark.read.format("delta").load("/delta/silver/product_product")
product_template_df =spark.read.format("delta").load("/delta/silver/product_template")
product_marque_df = spark.read.format("delta").load("/delta/silver/product_marque")
operating_unit_df = spark.read.format("delta").load("/delta/silver/operating_unit")
res_store_df = spark.read.format("delta").load("/delta/silver/res_store")
res_partner_df = spark.read.format("delta").load("/delta/silver/res_partner")
res_partner_category_df = spark.read.format("delta").load("/delta/silver/res_partner_category")
res_partner_title_df = spark.read.format("delta").load("/delta/silver/res_partner_title")
res_company_df = spark.read.format("delta").load("/delta/silver/res_company")
res_country_state_df = spark.read.format("delta").load("/delta/silver/res_country_state")
res_country_df = spark.read.format("delta").load("/delta/silver/res_country")
res_category =spark.read.format("delta").load("/delta/silver/res_category")
dsd_category = spark.read.format("delta").load("/delta/silver/dsd_category")
product_uom_df = spark.read.format("delta").load("/delta/silver/product_uom")
res_category_df = spark.read.format("delta").load("/delta/silver/res_category")
dsd_category_df = spark.read.format("delta").load("/delta/silver/dsd_category")
product_uom_categ_df = spark.read.format("delta").load("/delta/silver/product_uom_categ")
dsd_region_df = spark.read.format("delta").load("/delta/silver/dsd_region")

dsd_order_line_df= spark.read.format("delta").load("/delta/silver/dsd_order_line_df")
dsd_order_df= spark.read.format("delta").load("/delta/silver/dsd_order")
dsd_session_df= spark.read.format("delta").load("/delta/silver/dsd_session")
dsd_van_df= spark.read.format("delta").load("/delta/silver/dsd_van")
res_users_df= spark.read.format("delta").load("/delta/silver/res_users")




# Filter the DataFrame to keep only the rows where "category" is equal to "order"
sale_order_df = sale_order_df.filter(sale_order_df["category"] == "order")
sale_order_df = sale_order_df.filter(sale_order_df["state"] == "done" or sale_order_df["state"] == "done" or sale_order_df["state"] == "progress" or sale_order_df["state"] == "shipped" or sale_order_df["state"] == "waiting_ship_payment" or sale_order_df["state"] == "waiting_shipping" )


#les commandes 
join1_df = sale_order_line_df.join(sale_order_df, sale_order_line_df["order_id"] == sale_order_df["id"]) \
    .select(
            sale_order_line_df["product_id"],
            sale_order_line_df["product_uom"],
            sale_order_line_df["product_uom_qty"].alias("ordered_qty_uom"),
            sale_order_line_df["name"].alias("name_product"), 
            sale_order_line_df["product_qty"].alias("ordered_qty"),
            sale_order_line_df["price_unit"].alias("product_price_unit"),
            sale_order_line_df["purchase_price"].alias("product_purchase_price"), 
            sale_order_line_df["discount"], 
            sale_order_line_df["discount_amount"].alias("sale_discount_amount"), 
            sale_order_line_df["global_discount_amount"].alias("sale_discount_amount_global"), 
            sale_order_line_df["is_shipped"].alias("order_is_shipped"),
            sale_order_line_df["operating_unit_id"],
            sale_order_df["company_id"],
            sale_order_df["partner_id"].alias("client_id"), 
            sale_order_df["dsd_weight"].alias("order_weight"), 
            sale_order_df["request_date"].alias("order_request_date"), 
            sale_order_df["amount_total"].alias("order_amount_total"),
            sale_order_df["name"].alias("order_ref"),
            sale_order_df["state"].alias("order_state"),
            sale_order_df["date_order"]

        )

#Unités d'organisation et magasins 
join2_df = operating_unit_df.join(res_store_df, operating_unit_df["store_id"] == res_store_df["id"]) \
    .select(
            operating_unit_df["id"].alias("ou_id"),
            operating_unit_df["is_company"].alias("ou_is_company"),
            operating_unit_df["name"].alias("ou_name"),
            operating_unit_df["ou_warehouse"], 
            operating_unit_df["ou_store"],
            res_store_df["name"].alias("store_name"),
            res_store_df["code"].alias("store_code"),
            res_store_df["street"].alias("store_street"),
            res_store_df["city"].alias("store_city"),
            res_store_df["company_registry"].alias("store_company_registry") 
        
        )

#CLIENT
join3_df = res_partner_df.join(res_partner_title_df, res_partner_df["title"] == res_partner_title_df["id"]) \
                 .select(
                     res_partner_df["id"].alias("id_client"),
                     res_partner_df["is_company"].alias("client_is_company"),
                     res_partner_df["name"].alias("client_name"),
                     res_partner_df["street"].alias("client_street"),
                     res_partner_df["city"].alias("client_city"),
                     res_partner_df["zip"].alias("client_zip"),
                     #res_partner_df["state_id"].alias("client_state_id"),
                     #res_partner_df["Website "].alias("client_Website"),
                     res_partner_df["rc"].alias("client_registre_com"),
                     res_partner_df["ai"].alias("client_article_impo"),
                     res_partner_df["nif"].alias("client_num_id_fiscal"),
                     res_partner_df["nis"].alias("client_num_id_stat"),
                     res_partner_df["function"].alias("client_function"),
                     res_partner_df["identity_document"].alias("client_identity_document"),
                     res_partner_df["company_id"],
                     res_partner_df["bloque"].alias("client_is_blocked"),
                     res_partner_df["active"].alias("client_is_active"),
                     res_partner_df["dsd_customer"].alias("is_dsd_customer"),
                     res_partner_df["partner_longitude"].alias("client_longitude"),
                     res_partner_df["partner_latitude"].alias("client_latitude"),
                     res_partner_df["geo_point"].alias("client_geo_point"),
                     res_partner_title_df["name"].alias("client_title")
            )
                 
#sociétés 

join4_df = res_company_df.join(res_country_state_df, res_company_df["state_id"] == res_country_state_df["id"]) \
    .join(res_country_df, res_company_df["country_id"] == res_country_df["id"])\
    .select(
            res_company_df["name"].alias("company_name"),
            res_company_df["id"].alias("company_id"),
            res_company_df["code"].alias("company_code"),
            res_company_df["street"].alias("company_street"),
            res_company_df["city"].alias("company_city"),
            res_company_df["email"].alias("company_email"),
            res_company_df["company_registry"],
            res_company_df["ai"].alias("company_article_impo"),
            res_company_df["nif"].alias("company_num_id_fiscal"),
            res_company_df["nis"].alias("company_num_id_stat"),
            res_company_df["form_juridique"].alias("company_form_juridique"),
            res_country_state_df["name"].alias("country_state"),
            res_country_state_df["code"].alias("company_country_state_code"),
            res_country_df["name"].alias("country_name"),
            res_country_df["code"].alias("country_code")

        )
#PRODUITS
join5_df = product_product_df.join(product_template_df, product_product_df["product_tmpl_id"] == product_template_df["id"]) \
    .join(product_marque_df, product_template_df["marque_id"] == product_marque_df["id"]) \
    .join(res_category_df, product_product_df["categ1_id"] == res_category_df["id"] or product_product_df["categ2_id"] == res_category_df["id"] ) \
    .join(dsd_category_df, product_template_df["dsd_categ_id"] == dsd_category_df["id"]) \
    .select(
            product_template_df["name"].alias("product_name"),
            product_product_df["id"].alias("product_id"),
            product_template_df["sale_ok"],
            product_template_df["life_time"].alias("product_life_time"),
            product_product_df["remisable"].alias("product_is_remisable"),
            product_template_df["remise_max"].alias("product_remise_max"),
            product_product_df["is_kit"].alias("product_is_kit"),
            product_template_df["type"].alias("product_type"),
            product_template_df["uom_id"].alias("product_uom_id"),
            product_template_df["get_lot_price"].alias("product_get_lot_price"),
            product_template_df["description"].alias("product_description"),
            product_template_df["volume"].alias("product_volume"),
            product_template_df["weight_net"].alias("product_weight_net"),
            product_template_df["color"].alias("product_color"),
            product_product_df["use_quota"].alias("product_use_quota"),
            product_product_df["sequence"].alias("product_sequence"),
            product_template_df["available_in_dsd"].alias("product_available_in_dsd"),
            product_template_df["mes_type"].alias("product_type_mesure"),
            product_product_df["price1"].alias("product_price_detail"),
            product_product_df["price2"].alias("product_price_gros"),
            product_product_df["price3"].alias("product_price3"),
            product_product_df["price4"].alias("product_price4"),
            product_product_df["price5"].alias("product_price5"),
            product_marque_df["name"].alias("product_marque"),
            res_category_df["categ_field"].alias("product_category"),
            res_category_df["name"].alias("product_category_name")

        )
#unités de mesures 
join6_df = product_uom_df.join(product_uom_categ_df, product_uom_df["category_id"] == product_uom_categ_df["id"]) \
    .select(
            product_uom_df["id"].alias("uom_id"),
            product_uom_categ_df["name"].alias("product_uom_categ"),
            product_uom_df["name"].alias("product_uom"),
            product_uom_df["uom_type"].alias("uom_type"),
            
            
        )


#produits & unités de mesure 
join7_df = join5_df.join(join6_df, join5_df["product_uom_id"] == join6_df["uom_id"])\
        .select(join6_df["product_uom_categ"], join6_df["product_uom"],join6_df["uom_type"], 
            join5_df["product_name"], join5_df["product_id"],
            join5_df["sale_ok"],join5_df["product_life_time"],join5_df["product_is_remisable"],join5_df["product_remise_max"],join5_df["product_is_kit"],
            join5_df["product_type"],join5_df["product_uom_id"],join5_df["product_get_lot_price"],
            join5_df["product_description"],join5_df["product_volume"],join5_df["product_weight_net"],
            join5_df["product_color"], join5_df["product_use_quota"], join5_df["product_sequence"],
            join5_df["product_available_in_dsd"],join5_df["product_type_mesure"],
            join5_df["product_price_detail"],join5_df["product_price_gros"],join5_df["product_price3"],
            join5_df["product_price4"],join5_df["product_price5"],join5_df["product_marque"],join5_df["product_category"],join5_df["product_category_name"]

        )

#cmd B2B & operating_unit and company and client 
join8_df = join1_df.join(join2_df, join1_df["operating_unit_id"] == join2_df["ou_id"]) \
    .join(join3_df, join1_df["client_id"] == join3_df["id_client"]) \
    .join(join4_df, join1_df["company_id"] == join4_df["company_id"] ) \
    .select( join1_df["product_id"],join1_df["product_uom"],
            join1_df["ordered_qty_uom"],
            join1_df["order_state"],
            join1_df["order_ref"],
            join1_df["name_product"],join1_df["ordered_qty"],join1_df["product_price_unit"],join1_df["product_purchase_price"], join1_df["discount"], 
            join1_df["sale_discount_amount"], join1_df["sale_discount_amount_global"], join1_df["order_is_shipped"],
            join1_df["order_weight"], join1_df["order_request_date"], join1_df["order_amount_total"],join1_df["date_order"],
            join2_df["ou_is_company"],
            join2_df["ou_name"],join2_df["ou_warehouse"], join2_df["ou_store"],join2_df["store_name"],
            join2_df["store_code"],join2_df["store_city"],join2_df["store_company_registry"],
            join3_df["client_is_company"],join3_df["client_name"],join3_df["client_street"],join3_df["client_city"],join3_df["client_zip"],
        #res_partner_df["state_id"].alias("client_state_id"),
            #res_partner_df["Website "].alias("client_Website"),
            join3_df["client_registre_com"],join3_df["client_article_impo"], join3_df["client_num_id_fiscal"],join3_df["client_num_id_stat"],
            join3_df["client_function"],join3_df["client_identity_document"],join3_df["client_is_blocked"],join3_df["client_is_active"],
            join3_df["is_dsd_customer"],join3_df["client_longitude"],join3_df["client_latitude"],join3_df["client_geo_point"],join3_df["client_title"],
            join4_df["company_name"], join4_df["company_code"],join4_df["company_street"],join4_df["company_city"],join4_df["company_email"],
            join4_df["company_registry"],join4_df["company_article_impo"],join4_df["company_num_id_fiscal"],
            join4_df["company_num_id_stat"], join4_df["company_form_juridique"],
            join4_df["country_state"],join4_df["company_country_state_code"],join4_df["country_name"],join4_df["country_code"],



    )

#commandes et produits 

result_sale_df = join8_df.join(join7_df, join8_df["product_id"] == join7_df["product_id"]) \
    .select(
            join8_df["product_uom"],
            join8_df["ordered_qty_uom"],
            join8_df["order_ref"],
            join8_df["order_state"],
            join8_df["name_product"],join8_df["ordered_qty"],join8_df["product_price_unit"],join8_df["product_purchase_price"], join8_df["discount"], 
            join8_df["sale_discount_amount"], join8_df["sale_discount_amount_global"], join8_df["order_is_shipped"],
            join8_df["order_weight"], join8_df["order_request_date"], join8_df["order_amount_total"],join8_df["date_order"],
            join8_df["ou_is_company"],join8_df["ou_name"],join8_df["ou_warehouse"], join8_df["ou_store"],join8_df["store_name"],
            join8_df["store_code"],join8_df["store_city"],join8_df["store_company_registry"],
            join8_df["client_is_company"],join8_df["client_name"],join8_df["client_street"],join8_df["client_city"],join8_df["client_zip"],
        #res_partner_df["state_id"].alias("client_state_id"),
            #res_partner_df["Website "].alias("client_Website"),
            join8_df["client_registre_com"],join8_df["client_article_impo"], join8_df["client_num_id_fiscal"],join8_df["client_num_id_stat"],
            join8_df["client_function"],join8_df["client_identity_document"],join8_df["client_is_blocked"],join8_df["client_is_active"],
            join8_df["is_dsd_customer"],join8_df["client_longitude"],join8_df["client_latitude"],join8_df["client_geo_point"],join8_df["client_title"],
            join8_df["company_name"], join8_df["company_code"],join8_df["company_street"],join8_df["company_city"],join8_df["company_email"],
            join8_df["company_registry"],join8_df["company_article_impo"],join8_df["company_num_id_fiscal"],
            join8_df["company_num_id_stat"], join8_df["company_form_juridique"],join8_df["country_state"],join8_df["company_country_state_code"],join8_df["country_name"],join8_df["country_code"],
            join7_df["product_uom_categ"], join6_df["product_uom"],join6_df["uom_type"], 
            join7_df["product_name"],join7_df["sale_ok"],join7_df["product_life_time"],join7_df["product_is_remisable"],join7_df["product_remise_max"],join7_df["product_is_kit"],
            join7_df["product_type"],join7_df["product_uom_id"],join7_df["product_get_lot_price"],join7_df["product_description"],join7_df["product_volume"],join7_df["product_weight_net"],
            join7_df["product_color"], join7_df["product_use_quota"], join7_df["product_sequence"],join7_df["product_available_in_dsd"],join7_df["product_type_mesure"],
            join7_df["product_price_detail"],join7_df["product_price_gros"],join7_df["product_price3"],join7_df["product_price4"],join7_df["product_price5"],join7_df["product_marque"],join7_df["product_category"],join7_df["product_category_name"]

        )
        
# Adding the new column 'type_vente' with a constant value to all rows
type_vente_value = "b2b"
result_sale_df = result_sale_df.withColumn("type_vente", lit(type_vente_value))
result_sale_df.printSchema() #colums and their types 

result_sale_df.write.format("delta").save("/delta/sale_forecast")


######################dsd
#dsd orders 

dsd_session_df = dsd_session_df.filter(dsd_session_df["state"] == "closed" or dsd_session_df["state"] == "opened" or dsd_session_df["state"] == "confirmed" or dsd_session_df["state"] == "charging" or dsd_session_df["state"] == "closing_control" or dsd_session_df["state"] == "closing_exception")
#tournnéé et van 
join10_df = dsd_session_df.join(dsd_van_df, dsd_session_df["config_id"] == dsd_van_df["id"]) \
    .select(
            dsd_session_df["id"].alias("session_id"),
            dsd_session_df["name"].alias("session_name"),
            dsd_session_df["state"].alias("session_state"),
            dsd_session_df["start_at"].alias("session_start_at"),
            dsd_session_df["stop_at"].alias("session_stop_at"),
            dsd_van_df["name"].alias("van_name"),
            dsd_van_df["state"].alias("van_state"),
            dsd_van_df["store_id"]
    )

#session & van & store 
join11_df = join10_df.join(res_store_df, join10_df["store_id"] == res_store_df["id"]) \
    .select(
            join10_df["session_id"],
            join10_df["session_name"],
            join10_df["session_state"],
            join10_df["session_start_at"],
            join10_df["session_stop_at"],
            join10_df["van_name"],
            join10_df["van_state"],
            res_store_df["name"].alias("store_name"),
            res_store_df["code"].alias("store_code"),
            res_store_df["street"].alias("store_street"),
            res_store_df["city"].alias("store_city"),
            res_store_df["company_registry"].alias("store_company_registry") 
    )

#filter the dsd_orders to use 
dsd_order_df = dsd_order_df.filter(dsd_order_df["state"] == "done" or dsd_session_df["state"] == "invoiced" or dsd_session_df["state"] == "progress" or dsd_session_df["state"] == "paid" )
dsd_order_df = dsd_order_df.filter(dsd_order_df["is_return"] == "false" )
#dsd_order & session & partner & region & operating unit 
join12_df= dsd_order_df.join(join3_df, dsd_order_df["partner_id"] == join3_df["id_client"]) \
    .join (dsd_region_df, dsd_order_df["region_id"] == dsd_region_df["id"])\
    .join (join11_df, dsd_order_df["session_id"] == join11_df ["session_id"])\
    .join (operating_unit_df, dsd_order_df["operating_unit_id"] == operating_unit_df ["id"] )\
    .select(
        dsd_order_df["id"].alias("order_id"),
        dsd_order_df["name"].alias("order_ref"),
        dsd_order_df["state"].alias("order_state"),
        dsd_order_df["date_order"],
        dsd_order_df["is_preorder"],
        dsd_order_df["weight"].alias("order_weight"),
        dsd_order_df["volume"].alias("order_volume"),
        dsd_order_df["operating_unit_id"],
        dsd_order_df["user_id"],
        join3_df["client_registre_com"],join3_df["client_article_impo"], join3_df["client_num_id_fiscal"],join3_df["client_num_id_stat"],
        join3_df["client_function"],join3_df["client_identity_document"],join3_df["client_is_blocked"],join3_df["client_is_active"],
        join3_df["is_dsd_customer"],join3_df["client_longitude"],join3_df["client_latitude"],join3_df["client_geo_point"],join3_df["client_title"],
        dsd_region_df["name"].alias("region_name"),
        join11_df["session_name"],join11_df["session_state"],join11_df["session_start_at"],join11_df["session_stop_at"],join11_df["van_name"],join11_df["van_state"],join11_df["store_name"],
        join11_df["store_code"],join11_df["store_street"],join11_df["store_city"],join11_df["store_company_registry"],
        operating_unit_df["is_company"].alias("ou_is_company"),
        operating_unit_df["name"].alias("ou_name"),operating_unit_df["ou_warehouse"], operating_unit_df["ou_store"]
           
    )
#order_line & company & coountry & product
join13_df=  dsd_order_line_df.join(join4_df, dsd_order_df["company_id"] == join4_df["company_id"]) \
    .join (join5_df, dsd_order_line_df["product_id"] == join5_df["product_id"])\
    .select(
        dsd_order_line_df["qty"].alias("ordered_qty"),
        dsd_order_line_df["price_unit"].alias("product_price_unit"),
        dsd_order_line_df["discount"],
        join4_df["company_name"],join4_df["company_id"],join4_df["company_code"],join4_df["company_street"],join4_df["company_city"],join4_df["company_email"],join4_df["company_registry"],
        join4_df["company_article_impo"],join4_df["company_num_id_fiscal"],join4_df["company_num_id_stat"],join4_df["company_form_juridique"],join4_df["country_state"],
        join4_df["company_country_state_code"],join4_df["country_name"],join4_df["country_code"],
        join5_df["product_name"],join5_df["sale_ok"],join5_df["product_life_time"],join5_df["product_is_remisable"],
        join5_df["product_remise_max"],join5_df["product_is_kit"],join5_df["product_type"],join5_df["product_get_lot_price"],join5_df["product_description"],
        join5_df["product_volume"],join5_df["product_weight_net"],join5_df["product_color"],join5_df["product_use_quota"],join5_df["product_sequence"],join5_df["product_available_in_dsd"],
        join5_df["product_type_mesure"],join5_df["product_price_detail"],join5_df["product_price_gros"],join5_df["product_price3"],join5_df["product_price4"],join5_df["product_price5"],
        join5_df["product_marque"],join5_df["product_category"],join5_df["product_category_name"]
    )


result_dsd_df = join13_df.join(join12_df, join13_df["order_id"] == join12_df["order_id"]) \
     .select (
        join12_df["order_ref"],join12_df["order_state"],join12_df["date_order"],join12_df["is_preorder"],join12_df["order_weight"],join12_df["order_volume"],
        join12_df["client_registre_com"],join12_df["client_article_impo"], join12_df["client_num_id_fiscal"],join12_df["client_num_id_stat"],
        join12_df["client_function"],join12_df["client_identity_document"],join12_df["client_is_blocked"],join12_df["client_is_active"],
        join12_df["is_dsd_customer"],join12_df["client_longitude"],join12_df["client_latitude"],join12_df["client_geo_point"],join12_df["client_title"],
        join12_df["name"].alias("region_name"),
        join12_df["session_name"],join12_df["session_state"],join12_df["session_start_at"],join12_df["session_stop_at"],join12_df["van_name"],join12_df["van_state"],join12_df["store_name"],
        join12_df["store_code"],join12_df["store_street"],join12_df["store_city"],join12_df["store_company_registry"],
        join12_df["ou_is_company"],
        join12_df["ou_name"],join12_df["ou_warehouse"], join12_df["ou_store"],
        join13_df["name_product"],
        join13_df["ordered_qty"],
        join13_df["product_price_unit"],
        join13_df["discount"],
        join13_df["company_name"],join13_df["company_code"],join13_df["company_street"],join13_df["company_city"],join13_df["company_email"],join13_df["company_registry"],
        join13_df["company_article_impo"],join13_df["company_num_id_fiscal"],join13_df["company_num_id_stat"],join13_df["company_form_juridique"],join13_df["country_state"],
        join13_df["company_country_state_code"],join13_df["country_name"],join13_df["country_code"],
        join13_df["product_name"],join13_df["sale_ok"],join13_df["product_life_time"],join13_df["product_is_remisable"],
        join13_df["product_remise_max"],join13_df["product_is_kit"],join13_df["product_type"],join13_df["product_get_lot_price"],join13_df["product_description"],
        join13_df["product_volume"],join13_df["product_weight_net"],join13_df["product_color"],join13_df["product_use_quota"],join13_df["product_sequence"],join13_df["product_available_in_dsd"],
        join13_df["product_type_mesure"],join13_df["product_price_detail"],join13_df["product_price_gros"],join13_df["product_price3"],join13_df["product_price4"],join13_df["product_price5"],
        join13_df["product_marque"],join13_df["product_category"],join13_df["product_category_name"]

     )


type_vente_value = "dsd"
result_dsd_df= result_sale_df.withColumn("type_vente", lit(type_vente_value))
result_dsd_df.printSchema() #columns and their types 

#consolider ventes et dsd 
#schema evolution 
loans.write.option("mergeSchema","true").format("delta").mode("append").save(DELTALAKE_SILVER_PATH)













            


# Step 5: Save the final result DataFrame to the result Delta table
result_delta_table.alias("old_data").merge(
    result_sale_df.alias("new_data"),
    "old_data.primary_key_column = new_data.primary_key_column"
).whenMatchedUpdateAll().execute()


