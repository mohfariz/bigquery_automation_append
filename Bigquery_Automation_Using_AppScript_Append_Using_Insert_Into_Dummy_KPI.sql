function main() {
  // First, delete data for a specific date
  // deleteDataFromBigQuery();
  
  // Wait for a bit to ensure the delete operation has completed
  // Note: This is a simple approach; for production, consider implementing a more robust check.
  Utilities.sleep(10000); // Pause for 10 seconds
  
  // Then, append new data
  appendDataToBigQuery();
}

function deleteDataFromBigQuery() {
  var projectId = 'dataset-access';
  var datasetId = 'g-dataset-id-mart.merchant_sales'; // Dataset ID
  var tableId = 'enterprise_data_for_war_2023_monthly'; // Table ID

  var deleteQuery = `DELETE FROM \`g-dataset-id-mart.merchant_sales.enterprise_data_for_war_2023_monthly\` 
  WHERE TRUE 
  ----and booking_month >= date('2023-01-01')
  and booking_month =  DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), MONTH)
  `; ----// Delete data in sepecific interval time (In this case is current month data)
  
  const request = {
    query: deleteQuery,
    useLegacySql: false // Use standard SQL syntax
  };
  
  try {
    const response = BigQuery.Jobs.query(request, projectId);
    Logger.log(response); // Log the response for debugging
  } catch (error) {
    Logger.log('Error in deleteDataFromBigQuery:', error);
  }
}

function appendDataToBigQuery() {
  var projectId = 'dataset-access';
  var datasetId = 'g-dataset-id-mart.merchant_sales'; // Dataset ID
  var tableId = 'enterprise_data_for_war_2023_monthly'; // Table ID
  // Insert/Append query (simplified for brevity; replace with your actual query)
  var insertQuery = `
    INSERT INTO \`g-dataset-id-mart.merchant_sales.enterprise_data_for_war_2023_monthly\` as
    WITH
    dt AS (
    SELECT
    ---date('2023-01-01') as start_date,
    ----date('2024-09-30') as end_date
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), MONTH) AS start_date,
    DATE(DATE_SUB(CURRENT_DATE(), INTERVAL 1 day)) AS end_date
    ),

resto AS (
SELECT
DISTINCT
sales_code,
sales_name,
merchant_platform_outlet_id saudagar_id,
restaurant_id,
a.restaurant_uuid,
a.service_area_name,
a.sf_entity_id,
brand_id,
a.brand_name,
a.restaurant_name,
primary_cuisine_name,
a.sf_parent_entity_name,
classification_type_name,
general_group_name
FROM \`dataset-id-mart.food.detail_restaurant_profile_reference\` a
LEFT JOIN \`dataset-id-mart.merchant_sales.detail_merchant_sales_outlet_universe_reference\` b ON b.Food_id = a.restaurant_id

),

xrate AS (
SELECT
2024 AS year,
16000 AS rate
UNION ALL
2024 AS year,
15000 AS rate
UNION ALL
SELECT
2023 AS year,
15700 AS rate
),

economic_raw as (
    SELECT DISTINCT
    date(b.created_timestamp, "Asia/Jakarta") booking_date, 
    b.order_no,
    sum(case when rpl.budget_entity_name = 'AKAB' then rpl.Food_spend_amount*g.rate end) as rpl_overall,
    SUM(CASE WHEN program_name IN ('Project Abracadabra') THEN Food_spend_amount*g.rate END) AS abra_cost,
    SUM(CASE WHEN program_group_name IN ('Cofunding') THEN rpl.Food_spend_amount*g.rate END) AS rpl_cofunding,
  
    FROM \`dataset-id-mart.Food.detail_Food_booking\` b,  dt
    LEFT JOIN \`dataset-id-mart.food.detail_food_booking_cm\` a on a.order_no = b.order_no,  unnest(rpl_detail_list) rpl
    LEFT JOIN xrate g ON EXTRACT(year FROM a.booking_date) = g.year
    LEFT JOIN resto e on b.restaurant_id = e.restaurant_id

    WHERE TRUE
    and date(b.created_timestamp, "Asia/Jakarta") between start_date and end_date
    AND (classification_type_name = "ENTERPRISE")

 GROUP BY 1,2

    ),

  economic_booking as (
    SELECT DISTINCT
    date(b.created_timestamp, "Asia/Jakarta") booking_date,
    b.restaurant_id,
    a.order_no,
    a.cm_amount,
    a.cost_of_revenue_amount,
    a.gtv_amount,
    a.gross_take_preoffset_amount,
    a.merchant_commission_amount,
    a.mfp_markdown_amount,
    a.net_rpl_amount,
    a.gross_rpl_amount,
    c.rpl_overall as akab_rpl_overall,
    c.rpl_cofunding,
    c.abra_cost,
    a.gmv_amount

    FROM \`dataset-id-mart.food.detail_booking\` b,  dt
    LEFT JOIN \`dataset-id-mart.food.detail_food_booking_cm\` a on a.order_no = b.order_no
    LEFT JOIN economic_raw c on b.order_no = c.order_no and date(b.created_timestamp, "Asia/Jakarta") = c.booking_date
    LEFT JOIN resto e on b.restaurant_id = e.restaurant_id


WHERE TRUE
AND date(b.created_timestamp, "Asia/Jakarta") between start_date and end_date
AND Food_mode_name NOT IN ('FOOD-PICKUP')
AND (classification_type_name = "ENTERPRISE")
  
  ),


economic AS (
   SELECT DISTINCT booking_date,
    restaurant_id,
    SUM(IFNULL(cm_amount*g.rate,0)) cm,
    SUM(IFNULL(cost_of_revenue_amount*g.rate,0)) cor,
    SUM(IFNULL(gross_take_preoffset_amount*g.rate,0)) gt_preoffset,
    SUM(IFNULL(gtv_amount*g.rate,0)) gtv,
    SUM(IFNULL(net_rpl_amount*g.rate,0)) net_rpl,
    SUM(IFNULL(merchant_commission_amount*g.rate,0)) mc,
    SUM(IFNULL(gross_rpl_amount*g.rate,0)) gross_rpl,
    SUM(IFNULL(mfp_markdown_amount*g.rate,0)) mfp_md,
    COUNT(DISTINCT CASE WHEN mfp_markdown_amount > 0 THEN order_no END) co_mfp_non_sku,
    sum(akab_rpl_overall) akab_rpl_overall,
    sum(rpl_cofunding) rpl_cofunding,
    sum(abra_cost) abra_cost,
    SUM(DISTINCT CASE WHEN mfp_markdown_amount > 0 THEN gmv_amount * g.rate END) gmv_mfp_non_sku

FROM economic_booking a, dt
LEFT JOIN xrate g ON EXTRACT(year FROM a.booking_date) = g.year

GROUP BY 1,2
    ),


ads_impact AS (
SELECT DISTINCT
jakarta_data_date,
restaurant_uuid,
b.restaurant_id,
SUM(booked_revenue_amount) AS booked_revenue_amount,
SUM(billed_revenue_amount) AS realized_revenue_amount,
SUM(CASE WHEN member_type = "PAID" THEN booked_revenue_amount ELSE 0 END) AS booked_paid_revenue_amount,
SUM(CASE WHEN member_type = "PAID" THEN billed_revenue_amount ELSE 0 END) AS realized_paid_revenue_amount,
SUM(CASE WHEN member_type = "PAID" THEN realized_impression_count ELSE 0 END) AS realized_paid_impression_number,
SUM(CASE WHEN member_type = "PAID" THEN realized_click_count ELSE 0 END) AS realized_paid_click_number,
SUM(CASE WHEN member_type = "PAID" THEN direct_gmv_amount ELSE 0 END) AS realized_paid_direct_gmv_amount,
SUM(CASE WHEN member_type = "PAID" THEN direct_completed_order_count ELSE 0 END) AS realized_paid_direct_co_number,
SUM(CASE WHEN member_type = "PAID" THEN indirect_gmv_amount ELSE 0 END ) AS realized_paid_indirect_gmv_amount,
SUM(CASE WHEN member_type = "PAID" THEN indirect_completed_order_count ELSE 0 END) AS realized_paid_indirect_co_number,
SUM(CASE WHEN member_type = "PAID" THEN new_user_count ELSE 0 END) AS realized_paid_new_user_count,
SUM(CASE WHEN member_type = "PAID" THEN existing_user_count ELSE 0 END) AS realized_paid_existing_user_count,
SUM(CASE WHEN member_type = "PAID" THEN dormant_user_count ELSE 0 END) AS realized_paid_dormant_user_count,
IFNULL(SUM(IFNULL(CASE WHEN member_type = "PAID" AND inventory_name = "Food Search Ads" THEN billed_revenue_amount ELSE 0 END,0)),0) AS search_ads_realized_paid_revenue_amount,
IFNULL(SUM(IFNULL(CASE WHEN member_type = "PAID" AND inventory_name = "Food Category Ads" THEN billed_revenue_amount ELSE 0 END ,0)),0) AS category_ads_realized_paid_revenue_amount,
IFNULL(SUM(IFNULL(CASE WHEN member_type = "PAID" AND inventory_name = "Food CPC Fungible Ads" THEN billed_revenue_amount ELSE 0 END,0)),0) AS fungible_ads_realized_paid_revenue_amount,
IFNULL(SUM(IFNULL(CASE WHEN member_type = "PAID" AND inventory_name = "Food Top Banner" THEN billed_revenue_amount ELSE 0 END ,0)),0) AS top_banner_ads_realized_paid_revenue_amount,
IFNULL(SUM(IFNULL(CASE WHEN member_type = "PAID" AND inventory_name = "Partner Jempolan" THEN billed_revenue_amount ELSE 0 END ,0)),0) AS pj_ads_realized_paid_revenue_amount,
IFNULL(SUM(IFNULL(CASE WHEN member_type = "PAID" AND inventory_name = "Food Masthead" THEN billed_revenue_amount ELSE 0 END ,0)),0) AS masthead_ads_realized_paid_revenue_amount
FROM \`dataset-id-mart.merchant_promotion_platform.summary_ads_campaign_performance_daily\`, dt
LEFT JOIN resto b USING (restaurant_uuid)
WHERE TRUE
AND jakarta_data_date BETWEEN start_date AND end_date
AND LOWER(member_type) != "shadow"
AND jakarta_end_date >= start_date
AND
LOWER(CASE
WHEN sales_channel_name LIKE '%recommended%' THEN 'RP'
WHEN sales_channel_name LIKE '%cms%' THEN 'Sales'
WHEN sales_channel_name LIKE '%izard%' THEN 'Campaign_Wizard' ELSE 'OC' END)
IN ("rp","sales", "oc")
AND member_type IN ("PAID")
AND (classification_type_name = "ENTERPRISE")
GROUP BY
1, 2, 3
),

all_order AS(
SELECT DISTINCT
summary_data_date,
Food_id,
SUM(completed_Food_mfp_markdown_amount) completed_Food_mfp_markdown_amount,
SUM(all_Food_order_number) all_Food_order_number
FROM \`dataset-id-mart.merchant_sales.summary_merchant_sales_kr_analytic_daily\` a, dt
LEFT JOIN resto b ON a.Food_id = b.restaurant_id
WHERE summary_data_date BETWEEN start_date AND end_date
AND (b.classification_type_name = "ENTERPRISE")
GROUP BY 1,2
),

funnel_raw AS (
SELECT DISTINCT
summary_data_date,
restaurant_uuid,
restaurant_id,
MAX(profile_visit_over_merchant_count) profile_visit,
MAX(add_to_cart_over_merchant_count) add_to_cart,
FROM \`dataset-id-mart.merchant_sales.summary_merchant_sales_funnel_Food_apps_daily\`a, dt
LEFT JOIN resto c USING (restaurant_uuid)
WHERE
summary_data_date BETWEEN start_date AND end_date
AND (classification_type_name = "ENTERPRISE")
GROUP BY 1,2,3
),

funnel AS (
SELECT DISTINCT
summary_data_date,
restaurant_uuid,
restaurant_id,
SUM(profile_visit) profile_visit,
SUM(add_to_cart) add_to_Cart,
FROM funnel_raw
GROUP BY 1, 2, 3
),


operation AS (
SELECT
DISTINCT
jakarta_booking_created_date,
restaurant_id,
SUM(CASE WHEN a.status_id = '3' THEN TIMESTAMP_DIFF(picked_up_timestamp, restaurant_accepted_timestamp, minute) END) AS total_food_preparation_time_in_minute,
SUM(CASE WHEN a.status_id = '3' THEN a.booking_to_closed_duration_in_second END) AS total_completion_time_in_second,
SUM(CASE WHEN a.status_id = '3' THEN a.eta_in_minute END) AS total_eta_in_minute,
COUNT(DISTINCT CASE WHEN status_id = '3' AND (booking_to_closed_duration_in_second/60) > eta_in_minute + 5 THEN order_no END) AS total_late_delivery_order_number
FROM \`dataset-id-mart.Food.detail_Food_booking\` a, dt
LEFT JOIN resto c USING (restaurant_id)
WHERE
DATE(created_timestamp, 'Asia/Jakarta') BETWEEN start_date AND end_date
AND status_id = "3"
AND (classification_type_name = "ENTERPRISE")
GROUP BY 1,2
),

rating AS (
SELECT
DISTINCT DATE(DATETIME(a.created_timestamp, "Asia/Jakarta")) event_date,
restaurant_id,
SUM(original_rating_aggregated) rating_original,
SUM(app_rating_aggregated) rating_app,
SUM(rating_count) AS rating_count
FROM (
SELECT
created_timestamp,
restaurant_uuid,
rating_count*average_restaurant_rating original_rating_aggregated,
rating_count*restaurant_rating app_rating_aggregated,
rating_count
FROM
\`dataset-id-mart.Food.summary_rating_adjusted_by_restaurant_daily\`) a, dt
LEFT JOIN resto b ON a.restaurant_uuid = b.restaurant_uuid
WHERE
DATETIME(a.created_timestamp, "Asia/Jakarta") BETWEEN start_date AND end_date
AND (classification_type_name = "ENTERPRISE")
GROUP BY 1,2
),

impression AS(
SELECT
DISTINCT summary_data_date,
restaurant_uuid,
b.restaurant_id,
Food_shuffle_impression_count,
Food_search_impression_count,
Food_suggestion_impression_count,
Food_total_impression_count
FROM \`dataset-id-mart.merchant_sales.summary_merchant_sales_Food_apps_impression_daily\`a, dt
LEFT JOIN resto b USING (restaurant_uuid)
WHERE summary_data_date BETWEEN start_date AND end_date
AND (classification_type_name = "ENTERPRISE")
),

aov AS (
SELECT
*
FROM (
SELECT DISTINCT
summary_data_date,
a.Food_id,
CASE aov_distribution_name
WHEN 'a. 0-25k' THEN 'a_0_25k'
WHEN 'b. 25k-50k' THEN 'b_25_50k'
WHEN 'c. 50k-75k' THEN 'c_50_75k'
WHEN 'd. 75k-100k' THEN 'd_75_100k'
WHEN 'e. 100k-150k' THEN 'e_100_150k'
WHEN 'f. 150k-200k' THEN 'f_150_200k'
WHEN 'g. 200k-250k' THEN 'g_200_250k'
WHEN 'h. >= 250k' THEN 'h_over_250k'
END AS aov_range_alias,
COUNT(DISTINCT order_no) AS co
FROM (
SELECT
DISTINCT
jakarta_booking_created_date summary_data_date,
restaurant_id Food_id,
CASE WHEN COALESCE(t0.actual_gmv_amount, 0) < 25000 THEN "a. 0-25k"
WHEN COALESCE(t0.actual_gmv_amount, 0) >= 25000 AND COALESCE(t0.actual_gmv_amount, 0) < 50000 then 'b. 25k-50k'
WHEN COALESCE(t0.actual_gmv_amount, 0) >= 50000 AND COALESCE(t0.actual_gmv_amount, 0) < 75000 then 'c. 50k-75k'
WHEN COALESCE(t0.actual_gmv_amount, 0) >= 75000 AND COALESCE(t0.actual_gmv_amount, 0) < 100000 then 'd. 75k-100k'
WHEN COALESCE(t0.actual_gmv_amount, 0) >= 100000 AND COALESCE(t0.actual_gmv_amount, 0) < 150000 then 'e. 100k-150k'
WHEN COALESCE(t0.actual_gmv_amount, 0) >= 150000 AND COALESCE(t0.actual_gmv_amount, 0) < 200000 then 'f. 150k-200k'
WHEN COALESCE(t0.actual_gmv_amount, 0) >= 200000 AND COALESCE(t0.actual_gmv_amount, 0) < 250000 then 'g. 200k-250k'
WHEN COALESCE(t0.actual_gmv_amount, 0) >= 250000 THEN "h. >= 250k"
END AS aov_distribution_name,
order_no

FROM \`dataset-id-mart.Food.detail_Food_booking\` t0, dt
LEFT JOIN resto c USING (restaurant_id)
WHERE
DATE(created_timestamp, 'Asia/Jakarta') BETWEEN start_date AND end_date
AND status_id = "3"
AND (classification_type_name = "ENTERPRISE")
) a, dt
LEFT JOIN resto b ON a.Food_id = b.restaurant_id
--WHERE summary_data_date BETWEEN start_date AND end_date
--AND (classification_type_name = "ENTERPRISE")
--AND status_id = '3'
GROUP BY 1, 2, 3
) PIVOT ( SUM(co) FOR aov_range_alias IN ( 'a_0_25k',
'b_25_50k',
'c_50_75k',
'd_75_100k',
'e_100_150k',
'f_150_200k',
'g_200_250k',
'h_over_250k'))

),
promo_cat AS (
SELECT
*
FROM (
SELECT DISTINCT summary_data_date,
a.Food_id,
CASE promo_transaction_category_name
WHEN "1. organic" THEN "a_organic"
WHEN "2. cart" THEN "b_cart"
WHEN "3. sku" THEN "c_sku"
WHEN "4. voucher" THEN "d_voucher"
WHEN "5. cart_sku" THEN "e_cart_sku"
WHEN "6. cart_voucher" THEN "f_cart_voucher"
WHEN "7. sku_voucher" THEN "g_sku_voucher"
WHEN "8. sku_cart_voucher" THEN "h_sku_cart_voucher"
END AS promo_cat,
COUNT(DISTINCT order_no) AS co
FROM ((
SELECT
DISTINCT
jakarta_booking_created_date summary_data_date,
restaurant_id Food_id,
CASE WHEN COALESCE(sku_promotion_count, 0) > 0 AND COALESCE(total_campaign_discount_cart_amount, 0) > 0 AND COALESCE(voucher_discount_amount, 0) > 0 THEN "8. sku_cart_voucher"
WHEN COALESCE(sku_promotion_count, 0) = 0 AND COALESCE(total_campaign_discount_cart_amount, 0) > 0 AND COALESCE(voucher_discount_amount, 0) > 0 THEN "6. cart_voucher"
WHEN COALESCE(sku_promotion_count, 0) > 0 AND COALESCE(total_campaign_discount_cart_amount, 0) = 0 AND COALESCE(voucher_discount_amount, 0) > 0 THEN "7. sku_voucher"
WHEN COALESCE(sku_promotion_count, 0) > 0 AND COALESCE(total_campaign_discount_cart_amount, 0) > 0 AND COALESCE(voucher_discount_amount, 0) = 0 THEN "5. cart_sku"
WHEN COALESCE(sku_promotion_count, 0) > 0 AND COALESCE(total_campaign_discount_cart_amount, 0) = 0 AND COALESCE(voucher_discount_amount, 0) = 0 THEN "3. sku"
WHEN COALESCE(sku_promotion_count, 0) = 0 AND COALESCE(total_campaign_discount_cart_amount, 0) > 0 AND COALESCE(voucher_discount_amount, 0) = 0 THEN "2. cart"
WHEN COALESCE(sku_promotion_count, 0) = 0 AND COALESCE(total_campaign_discount_cart_amount, 0) = 0 AND COALESCE(voucher_discount_amount, 0) > 0 THEN "4. voucher"
WHEN COALESCE(sku_promotion_count, 0) = 0 AND COALESCE(total_campaign_discount_cart_amount, 0) = 0 AND COALESCE(voucher_discount_amount, 0) = 0 THEN "1. organic"
END AS promo_transaction_category_name,
order_no

FROM \`dataset-id-mart.Food.detail_Food_booking\` t0, dt
LEFT JOIN resto c USING (restaurant_id)
WHERE
DATE(created_timestamp, 'Asia/Jakarta') BETWEEN start_date AND end_date
AND status_id = "3"
AND (classification_type_name = "ENTERPRISE")
) )

a, dt
LEFT JOIN resto b ON a.Food_id = b.restaurant_id
--WHERE summary_data_date BETWEEN start_date AND end_date
--AND (classification_type_name = "ENTERPRISE")
--AND status_id = '3'
GROUP BY
1,2,3) PIVOT ( SUM(co) FOR promo_cat IN ( "a_organic",
'b_cart',
'c_sku',
'd_voucher',
'e_cart_sku',
'f_cart_voucher',
'g_sku_voucher',
'h_sku_cart_voucher' ))

),
raw AS (
SELECT DISTINCT
DATE_TRUNC(
COALESCE(a.summary_data_date, b.booking_date, d.summary_data_date, e.jakarta_data_date, f.summary_data_date, g.summary_data_date, h.summary_data_date, i.summary_data_date, j.jakarta_booking_created_date), month)
booking_month,
c.saudagar_id,
COALESCE(a.Food_id,b.restaurant_id, d.Food_id, e.restaurant_id, f.restaurant_id, g.restaurant_id, h.Food_id, i.Food_id, j.restaurant_id) Food_id,
primary_cuisine_name,
c.restaurant_name,
c.service_area_name,
c.brand_id,
c.brand_name,
sf_entity_id,
sf_parent_entity_name,
c.general_group_name,
"ENTERPRISE" classification_type_name,
---Matrics
SUM(IFNULL(completed_Food_gmv_amount, 0)) gmv,
SUM(IFNULL(completed_Food_order_number, 0)) co,
SUM(IFNULL(d.completed_Food_mfp_markdown_amount, 0)) mfp_markdown,
SUM(IFNULL(completed_Food_mfp_sku_amount, 0)) mfp_sku,
SUM(IFNULL(completed_Food_mfp_voucher_amount, 0)) mfp_voucher,
SUM(IFNULL(co_mfp_non_sku, 0)) co_mfp_non_sku,
SUM(IFNULL(a.all_Food_order_number, 0)) total_order,
---economics gfgm
SUM(IFNULL(cm, 0)) cm,
SUM(IFNULL(cor, 0)) cor,
SUM(IFNULL(gt_preoffset, 0)) gt,
SUM(IFNULL(gtv, 0)) gtv,
SUM(IFNULL(net_rpl, 0)) net_rpl,
SUM(IFNULL(mc, 0)) mc,
SUM(IFNULL(gross_rpl, 0)) gross_rpl,
SUM(IFNULL(completed_merchant_commission_amount, 0)) est_mc,
SUM(IFNULL(completed_Food_estimated_rpl_amount, 0)) est_rpl,
---operational
SUM(IFNULL(total_food_preparation_time_in_minute, 0)) fpt_min,
SUM(IFNULL(total_completion_time_in_second, 0)) completion_time_sec,
SUM(IFNULL(total_eta_in_minute, 0)) eta_min,
SUM(IFNULL(total_late_delivery_order_number, 0)) late_delivery_order,
SUM(IFNULL(rating_original, 0)) total_aggregated_rating,
SUM(IFNULL(rating_app, 0)) total_app_restaurant_rating,
SUM(IFNULL(rating_count, 0)) total_rating_number,
---Ads
SUM(IFNULL(ads_booked_amount, 0)) ads_booked_revenue_amount,
SUM(IFNULL(ads_realized_amount, 0)) ads_realized_revenue_amount,
SUM(IFNULL(ads_booked_paid_revenue_amount, 0)) ads_booked_paid_revenue_amount,
SUM(IFNULL(ads_realized_paid_revenue_amount, 0)) ads_realized_paid_revenue_amount,
SUM(IFNULL(search_ads_realized_paid_revenue_amount, 0)) search_ads_realized_paid_revenue_amount,
SUM(IFNULL(category_ads_realized_paid_revenue_amount, 0)) category_ads_realized_paid_revenue_amount,
SUM(IFNULL(fungible_ads_realized_paid_revenue_amount, 0)) fungible_ads_realized_paid_revenue_amount,
SUM(IFNULL(top_banner_ads_realized_paid_revenue_amount, 0)) top_banner_ads_realized_paid_revenue_amount,
SUM(IFNULL(pj_ads_realized_paid_revenue_amount, 0)) pj_ads_realized_paid_revenue_amount,
SUM(IFNULL(masthead_ads_realized_paid_revenue_amount, 0)) masthead_ads_realized_paid_revenue_amount,
---ads impact
SUM(IFNULL(realized_paid_impression_number, 0)) realized_paid_impression_number,
SUM(IFNULL(realized_paid_click_number, 0)) realized_paid_click_number,
SUM(IFNULL(realized_paid_direct_gmv_amount, 0)) realized_paid_direct_gmv_amount,
SUM(IFNULL(realized_paid_direct_co_number, 0)) realized_paid_direct_co_number,
SUM(IFNULL(realized_paid_indirect_gmv_amount, 0)) realized_paid_indirect_gmv_amount,
SUM(IFNULL(realized_paid_indirect_co_number, 0)) realized_paid_indirect_co_number,
SUM(IFNULL(realized_paid_new_user_count, 0)) realized_paid_new_user_count,
SUM(IFNULL(realized_paid_existing_user_count, 0)) realized_paid_existing_user_count,
SUM(IFNULL(realized_paid_dormant_user_count, 0)) realized_paid_dormant_user_count,
---funnel
SUM(IFNULL(profile_visit,0)) profile_visit,
SUM(IFNULL(add_to_cart,0)) add_to_cart,
--- impression
SUM(IFNULL(Food_shuffle_impression_count, 0)) Food_shuffle_impression_count,
SUM(IFNULL(Food_search_impression_count, 0)) Food_search_impression_count,
SUM(IFNULL(Food_suggestion_impression_count, 0)) Food_suggestion_impression_count,
SUM(IFNULL(Food_total_impression_count, 0)) Food_total_impression_count,
---aov distribution
SUM(IFNULL(a_0_25k, 0)) a_0_25k,
SUM(IFNULL(b_25_50k, 0)) b_25_50k,
SUM(IFNULL(c_50_75k, 0)) c_50_75k,
SUM(IFNULL(d_75_100k, 0)) d_75_100k,
SUM(IFNULL(e_100_150k, 0)) e_100_150k,
SUM(IFNULL(f_150_200k, 0)) f_150_200k,
SUM(IFNULL(g_200_250k, 0)) g_200_250k,
SUM(IFNULL(h_over_250k, 0)) h_over_250k,
----promo category,
SUM(IFNULL(a_organic,0)) AS a_organic,
SUM(IFNULL(b_cart,0)) AS b_cart,
SUM(IFNULL(c_sku,0)) AS c_sku,
SUM(IFNULL(d_voucher,0)) AS d_voucher,
SUM(IFNULL(e_cart_sku,0)) AS e_cart_sku,
SUM(IFNULL(f_cart_voucher,0)) AS f_cart_voucher,
SUM(IFNULL(g_sku_voucher,0)) AS g_sku_voucher,
SUM(IFNULL(h_sku_cart_voucher,0)) AS h_sku_cart_voucher,
MAX(COALESCE(a.summary_data_date, b.booking_date, d.summary_data_date, e.jakarta_data_date, f.summary_data_date, g.summary_data_date, h.summary_data_date, i.summary_data_date, j.jakarta_booking_created_date))
lates_update_date,
c.sales_code,
c.sales_name,
sum(akab_rpl_overall) akab_rpl_overall,
sum(rpl_cofunding) rpl_cofunding,
sum(abra_cost) abra_cost,
sum(gmv_mfp_non_sku) gmv_from_mfp

FROM \`dataset-id-mart.merchant_sales.summary_merchant_sales_kr_analytic_daily\` a
FULL JOIN economic b ON a.Food_id = b.restaurant_id AND a.summary_data_date = b.booking_date
FULL JOIN all_order d ON a.Food_id = d.Food_id AND a.summary_data_date = d.summary_data_date
FULL JOIN ads_impact e ON a.Food_id = e.restaurant_id AND a.summary_data_date = e.jakarta_data_date
FULL JOIN funnel f ON a.Food_id = f.restaurant_id AND a.summary_data_date = f.summary_data_date
FULL JOIN impression g ON a.Food_id = g.restaurant_id AND a.summary_data_date = g.summary_data_date
FULL JOIN aov h ON a.Food_id = h.Food_id AND a.summary_data_date = h.summary_data_date
FULL JOIN promo_cat i ON a.Food_id = i.Food_id AND a.summary_data_date = i.summary_data_date
FULL JOIN operation j ON a.Food_id = j.restaurant_id AND a.summary_data_date = j.jakarta_booking_created_date
FULL JOIN rating k ON a.Food_id = k.restaurant_id AND a.summary_data_date = k.event_date
FULL JOIN resto c ON a.Food_id=c.restaurant_id,
dt
WHERE
COALESCE(a.summary_data_date, b.booking_date, d.summary_data_date, e.jakarta_data_date, f.summary_data_date, g.summary_data_date, h.summary_data_date, i.summary_data_date)
BETWEEN start_date AND dt.end_date
AND (c.classification_type_name = "ENTERPRISE")
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,sales_code, sales_name
)
SELECT
*
FROM
raw`;
  
  var request = {
    query: insertQuery,
    useLegacySql: false,
    destinationTable: {
      projectId: projectId,
      datasetId: datasetId,
      tableId: tableId
    },
    writeDisposition: 'WRITE_APPEND', // Append the table data 
    location: 'US' // Specify the dataset location if known
  };

  try {
    var job = BigQuery.Jobs.query(request, projectId);
    var jobId = job.jobReference.jobId;
    // Poll for the job status until it is done
    let status = job.status;
    while (status && status.state !== 'DONE') {
      Utilities.sleep(100000); // Wait for 10 seconds before checking the job status again
      const jobCheck = BigQuery.Jobs.get(projectId, jobId);
      status = jobCheck.status;
    if (status.errorResult) {
        Logger.log('Job failed with error: ' + status.errorResult.message);
        sendEmail(false, status.errorResult.message);
        return;
      }
    }
    
    Logger.log('Query job completed successfully.');
    sendEmail(true); // Success
  } catch (e) {
    Logger.log('Error running query: ' + e.toString());
    sendEmail(false, e.toString()); // Failure with error message
  }
}

function sendEmail(success, errorMessage) {
  var subject, body;
  if (success) {
    subject = "BigQuery Job Completion Notification";
    body = "The BigQuery job has completed successfully.";
  } else {
    subject = "BigQuery Job Failure Notification";
    body = "The BigQuery job failed to complete." + (errorMessage ? " Error: " + errorMessage : "");
  }

  MailApp.sendEmail({
    to: "your_email@host.com",
    subject: subject,
    body: body,
  });
}