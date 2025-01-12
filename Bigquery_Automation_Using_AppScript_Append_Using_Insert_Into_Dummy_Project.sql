function main() {
  // First, delete data for a specific date
  deleteDataFromBigQuery();
  
  // Wait for a bit to ensure the delete operation has completed
  // Note: This is a simple approach; for production, consider implementing a more robust check.
  Utilities.sleep(10000); // Pause for 10 seconds
  
  // Then, append new data
  appendDataToBigQuery();
}

function deleteDataFromBigQuery() {
  var projectId = 'dataset-project-access';
  var datasetId = 'g-dataset-id-mart.merchant_sales'; // Dataset ID
  var tableId = 'daily_t3_cities_food_performance'; // Table ID

  var deleteQuery = `DELETE FROM \`g-dataset-id-mart.merchant_sales.ent_food_pas_performance\` WHERE date_trunc(booking_date, month) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), MONTH)`;
  
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
  var projectId = 'dataset-project-access';
  var datasetId = 'g-dataset-id-mart.merchant_sales'; // Dataset ID
  var tableId = 'daily_t3_cities_food_performance'; // Table ID
  // Insert/Append query (simplified for brevity; replace with your actual query)
  var insertQuery = `
    INSERT INTO \`g-dataset-id-mart.merchant_sales.ent_food_pas_performance\`
WITH
  dt AS (
  SELECT
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), MONTH) AS start_date,
    DATE(DATE_SUB(CURRENT_DATE(), INTERVAL 1 day)) AS end_date ),
outlet AS (
  SELECT
    DISTINCT sf_outlet_id,
    outlet_id,
    sf_account_id AS entity_id, 
  FROM \`dataset-id-presentation.sales_platform.dim_outlet\` AS a
  
  WHERE
    DATE(effective_timestamp, 'Asia/Jakarta')<=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND DATE(COALESCE(expired_timestamp,CURRENT_TIMESTAMP()),"Asia/Jakarta")> DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND NOT a.deleted_flag
    AND IFNULL(a.duplicate_flag,FALSE) IS FALSE 
    --and sf_account_id = '0010I00001fRfBgQAK'
    ),
  
  product_outlet AS (
  SELECT
    DISTINCT sf_outlet_id,
    record_type_name_detail,
    parent_id,
  FROM \`dataset-id-presentation.sales_platform.dim_account\` AS a
  WHERE
    LOWER(record_type_name_detail) LIKE "%product%outlet%food%"
    AND DATE(effective_timestamp, 'Asia/Jakarta')<=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND DATE(COALESCE(expired_timestamp,CURRENT_TIMESTAMP()),"Asia/Jakarta")> DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND IFNULL(deleted_flag,FALSE) IS FALSE
    AND billing_group_entity_id IS NOT NULL ),
  
  billing_group AS (
  SELECT
    a.sf_account_id,
    parent_entity_detail_name,
  FROM \`dataset-id-presentation.sales_platform.dim_account\` a
  WHERE
    TRUE
    AND UPPER(record_type_name_detail) = "BILLING GROUP"
    AND service_product_list_name = 'FOOD'
    AND DATE(effective_timestamp, 'Asia/Jakarta')<=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND DATE(COALESCE(expired_timestamp,CURRENT_TIMESTAMP()),"Asia/Jakarta")> DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND IFNULL(deleted_flag, FALSE) IS FALSE ),
  
    entity AS (
  SELECT
    a.sf_account_id,
    sf_account_name parent_entity_detail_name,
  FROM \`dataset-id-presentation.sales_platform.dim_account\` a
  WHERE
    TRUE
    AND UPPER(record_type_name_detail) = "Entity"
    AND service_product_list_name = 'GO-FOOD'
    AND DATE(effective_timestamp, 'Asia/Jakarta')<=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND DATE(COALESCE(expired_timestamp,CURRENT_TIMESTAMP()),"Asia/Jakarta")> DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND IFNULL(deleted_flag, FALSE) IS FALSE ),
  
  bg AS (
  SELECT
    DISTINCT a.outlet_id,
    a.entity_id AS sf_entity_id,
    d.parent_entity_detail_name,
  FROM outlet AS a
  LEFT JOIN product_outlet AS b ON a.sf_outlet_id = b.sf_outlet_id
  LEFT JOIN billing_group AS c ON c.sf_account_id = b.parent_id
  LEFT JOIN Entity AS d ON d.sf_account_id = b.parent_id
  WHERE
    TRUE
    AND LOWER(record_type_name_detail) LIKE "%product%outlet%food%"
    ----and a.entity_id = '0010I00001buWnjQAE'
  ),

resto AS (
    SELECT
    DISTINCT
    merchant_platform_outlet_id saudagar_id,
    restaurant_id,
    a.restaurant_uuid,
    a.service_area_name,
    coalesce(c.sf_entity_id, a.sf_entity_id) sf_entity_id,
    brand_id,
    a.brand_name,
    a.restaurant_name,
    primary_cuisine_name,
    coalesce(parent_entity_detail_name, sf_parent_entity_name) sf_parent_entity_name,
    classification_type_name,
    general_group_name,
    subdistrict_name,
    regency_name,
    sales_region_name
FROM \`dataset-id-mart.food.detail_restaurant_profile_reference\` a
LEFT JOIN \`dataset-id-mart.merchant_sales.detail_merchant_sales_outlet_universe_reference\` b ON b.food_id = a.restaurant_id
left join bg c on a. merchant_platform_outlet_id = c.outlet_id
LEFT JOIN \`dataset-id-mart.cartography.detail_s2_14_reference\` loc on a.location_s2id_14 = loc.location_s2id_14
),
raw as (
SELECT DISTINCT
DATE(a.created_timestamp, "Asia/Jakarta") booking_date,
general_group_name,
b.sf_entity_id,
b.sf_parent_entity_name,
b.restaurant_id,
b.restaurant_name,
b.brand_id,
b.brand_name,
subdistrict_name, 
regency_name, 
b.service_area_name, 
b.sales_region_name,
    CASE when b.service_area_name IN ("PALANGKARAYA","TASIKMALAYA","JEMBER","MANADO","PURWOKERTO","MADIUN","SUKABUMI","KEDIRI","MATARAM","TANJUNG PINANG","GARUT","TEGAL","BANDA ACEH","PASURUAN","JAMBI","MAGELANG","PEKALONGAN","BUKIT TINGGI","MOJOKERTO","BANYUWANGI","PURWAKARTA","CILACAP","AMBON","BELITUNG","BERAU","BOJONEGORO","GORONTALO","JAYAPURA","JOMBANG","KEBUMEN","KENDARI","KISARAN","KUDUS","KUPANG","MADURA","MERAUKE","METRO","PALU","PANGKAL PINANG","PAREPARE","PEMATANGSIANTAR","PROBOLINGGO","SORONG","SUBANG","SUMEDANG","TARAKAN","TERNATE") then "T3 Cities" else "Other Cities" end city_category,
    case when b.service_area_name IN ("PALANGKARAYA","TASIKMALAYA","JEMBER","MANADO","PURWOKERTO","MADIUN","SUKABUMI","KEDIRI","MATARAM","TANJUNG PINANG","GARUT","TEGAL","BANDA ACEH","PASURUAN","JAMBI","MAGELANG","PEKALONGAN","BUKIT TINGGI","MOJOKERTO","BANYUWANGI","PURWAKARTA","CILACAP") then "Focus 22 T3" else "Other Catgories" end T3_categories,
    case when b.service_area_name iN ("PASURUAN","GARUT","PALANGKARAYA","TASIKMALAYA","PURWOKERTO") then "Top 5 T3 cities" else "Others" end top_5_category,
    
    COUNT(DISTINCT (case when a.status_id = '3' then a.customer_id end)) AS mau,
    COUNT(DISTINCT (CASE WHEN a.status_id = '3' THEN a.order_no END)) AS trx,
    COUNT(DISTINCT (a.order_no)) AS total_order,
    sum(CASE WHEN a.status_id = '3' THEN a.actual_gmv_amount END) AS food_gmv,
    sum(CASE WHEN a.status_id = '3' then c.sku_promo_burnt end) AS sku_promo_burnt,
    sum(CASE WHEN a.status_id = '3' then c.markdown_mfp_burnt end) AS markdown_mfp_burnt,
    sum(CASE WHEN a.status_id = '3' then c.voucher_mfp_burnt end) AS voucher_mfp_burnt,
    sum(CASE WHEN a.status_id = '3' then c.total_order_mfp_burnt end) AS total_order_mfp_burnt,
    sum(ifnull(case when status_id = '3' then estimated_rpl_total_amount end,0)) estimated_rpl_total_amount,
    sum(ifnull(case when status_id = '3' then rpl_voucher_amount end,0)) rpl_voucher_amount,
    sum(ifnull(case when status_id = '3' then rpl_markdown_delivery_subsidy_amount end,0)) rpl_markdown_delivery_subsidy_amount,
    sum(ifnull(case when status_id = '3' then rpl_markdown_direct_discount_amount end,0)) rpl_markdown_direct_discount_amount,
    sum(ifnull(case when status_id = '3' then rpl_subscription_amount end,0)) rpl_subscription_amount,
    sum(ifnull(case when status_id = '3' then rpl_markdown_gopay_coins_issuance_amount end,0)) rpl_markdown_gopay_coins_issuance_amount,
    sum(ifnull(case when status_id = '3' then rpl_gopay_coins_redemption_amount end,0)) rpl_gopay_coins_redemption_amount,
    sum(ifnull(case when status_id = '3' then rpl_mission_cashback_amount end,0)) rpl_mission_cashback_amount,
    sum(ifnull(case when status_id = '3' then rpl_invoice_adjustment_amount end,0)) rpl_invoice_adjustment_amount,
    sum(ifnull(case when status_id = '3' then rpl_dynamic_platform_fee_discount_amount end,0))rpl_dynamic_platform_fee_discount_amount,
    sum(ifnull(case when status_id = '3' then economical_delivery_discount_amount end,0)) economical_delivery_discount_amount,
    count(distinct case when status_id = '3' and ifnull(c.markdown_mfp_burnt,0) > 0 then a.order_no end) co_from_mfp_markdown,
    sum(case when status_id = '3' and ifnull(c.markdown_mfp_burnt,0) > 0 then a.normalized_gmv_amount end) gmv_from_mfp_markdown

FROM \`dataset-id-mart.food.detail_food_booking\` a, dt
 LEFT JOIN
  (
    SELECT
      restaurant_type AS classification,
      item_promotion_burnt_amount AS sku_promo_burnt,
      customer_id,
      subscription_flag AS with_subscription,
      cbv_amount AS cbv,
      bundle_flag AS with_bundle,
      order_no,
      payment_type AS payment_method,
      campaign_discount_amount AS total_markdown_discount,
      campaign_discount_burnt_amount AS markdown_mfp_burnt,
      jakarta_booking_date AS booking_date,
      partner_flag,
      voucher_burnt_amount AS voucher_mfp_burnt,
      total_order_burnt_amount AS total_order_mfp_burnt,
      gmv_amount AS gmv,
      restaurant_name AS merchant_name,
      restaurant_uuid AS merchant_uuid,
      service_area_name,
      brand_uuid AS brand_id,
      goresto_flag,
      brand_name
    FROM
      \`dataset-id-mart.food.detail_mfp_transaction\`, dt where jakarta_booking_date between start_date and end_date
  ) AS c
   ON a.order_no = c.order_no
left join resto b using(restaurant_id)
where
DATE(a.created_timestamp, "Asia/Jakarta") between start_date and end_date
--AND status_id = '3'
and food_pas_flag is true
and classification_type_name = "ENTERPRISE"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)

SELECT * FROM raw`;
  
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
    to: "mohammad.fariz@gojek.com",
    subject: subject,
    body: body,
  });
}