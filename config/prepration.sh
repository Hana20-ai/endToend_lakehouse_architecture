#Renaming the columns res_partner 
res_partner: >>> df1 = df1.withColumnRenamed("name", "name_partner") \
... .withColumnRenamed("is_company","partner_is_company") \
... .withColumnRenamed("category_id","partner_category_id") \
... .withColumnRenamed("street","partner_street") \
... .withColumnRenamed("city","partner_city") \
... .withColumnRenamed("zip","partner_zip") \
... .withColumnRenamed("zip","partner_zip") \
... .withColumnRenamed("website","partner_website") \
... .withColumnRenamed("rc","partner_company_registry") \
... .withColumnRenamed("nif","partner_nif") \
... .withColumnRenamed("nis","partner_nis") \
... .withColumnRenamed("function","partner_function") \
... .withColumnRenamed("phone","partner_phone") \
... .withColumnRenamed("mobile","partner_mobile") \
... .withColumnRenamed("title","partner_title") \
... .withColumnRenamed("identity_document","partner_identity_document") \
... .withColumnRenamed("user_id","partner_user_id") \
... .withColumnRenamed("company_id","partner_company_id") \
... .withColumnRenamed("dsd_region_id","partner_dsd_region_id") \
... .withColumnRenamed("geo_point","partner_geo_point") \
... .withColumnRenamed("current_competitor_id ","partner_current_competitor_id")
… .withColumnRenamed("supplier","partner_is_supplier") \
... .withColumnRenamed("prospect","partner_is_prospect")


