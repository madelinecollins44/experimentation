view: active_experiment_summary {
  sql_table_name: `etsy-data-warehouse-prod.rollups.active_experiment_summary` ;;

  dimension: bucketing_type {
    type: string
    sql: ${TABLE}.bucketing_type ;;
  }
  # dimension: catapult_link {
  #   type: string
  #   sql: 'https://atlas.etsycorp.com/catapult/' || ${launch_id} ;;
  #   link: {
  #     label: "Catapult link"
  #     url: "https://atlas.etsycorp.com/catapult/{{launch_id._value}}"
  #   }
  # }
  dimension: days_running {
    type: number
    sql: ${TABLE}.days_running ;;
  }
  dimension: group_name {
    type: string
    sql: ${TABLE}.group_name ;;
  }
  dimension: hypothesis {
    type: string
    sql: ${TABLE}.hypothesis ;;
  }
  dimension: initiative {
    type: string
    sql: ${TABLE}.initiative ;;
  }
  dimension: number_of_treatments {
    type: number
    sql: ${TABLE}.number_of_treatments ;;
  }
  dimension: name_of_treatment {
    type: string
    sql: ${TABLE}.name_of_treatment ;;
  }
  dimension_group: last_run_date {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.last_run_date ;;
  }
  dimension: launch_id {
    type: number
    sql: ${TABLE}.launch_id ;;
  }
  dimension: launch_percentage {
    type: number
    sql: ${TABLE}.launch_percentage ;;
  }
  dimension: outcome {
    type: string
    sql: ${TABLE}.outcome ;;
  }
  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }
  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }
  dimension: significance_pages_per_unit{
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Pages per Unit"
    sql: ${TABLE}.significance_pages_per_unit;;
  }

  dimension: significance_ads_cvr {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Ads CVR"
    sql: ${TABLE}.significance_ads_cvr ;;
  }
  dimension: significance_aov {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- AOV"
    sql: ${TABLE}.significance_aov ;;
  }
  dimension: significance_atc {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- ATC"
    sql: ${TABLE}.significance_atc ;;
  }
  dimension: significance_bounces {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Bounces"
    sql: ${TABLE}.significance_bounces ;;
  }
  dimension: significance_checkout_start {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Checkout Starts"
    sql: ${TABLE}.significance_checkout_start ;;
  }
  dimension: significance_conversion_rate {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- CR"
    sql: ${TABLE}.significance_conversion_rate ;;
  }
  dimension: significance_engaged_visit {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Engaged Visits"
    sql: ${TABLE}.significance_engaged_visit ;;
  }
  dimension: significance_gms_per_unit {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- GMS Per Unit"
    sql: ${TABLE}.significance_gms_per_unit ;;
  }
  dimension: significance_mean_osa_revenue {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Mean OSA Revenue"
    sql: ${TABLE}.significance_mean_osa_revenue ;;
  }
  dimension: significance_mean_prolist_spend {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Mean Prolist Spend"
    sql: ${TABLE}.significance_mean_prolist_spend ;;
  }
  dimension: significance_mean_visits {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Mean Visits"
    sql: ${TABLE}.significance_mean_visits ;;
  }
  dimension: significance_ocb {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Orders Per Converting Browser"
    sql: ${TABLE}.significance_ocb ;;
  }
  dimension: significance_opu {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Orders Per User"
    sql: ${TABLE}.significance_opu ;;
  }
  dimension: significance_percent_error_pg_view {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- % with Error Pageview"
    sql: ${TABLE}.significance_percent_error_pg_view ;;
  }
  dimension: significance_winsorized_acxv {
    type: yesno
    group_label: "Stat Sig Change"
    label: "Stat Sig Change- Winsorized ACXV"
    sql: ${TABLE}.significance_winsorized_acxv ;;
  }
  dimension_group: start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.start_date ;;
  }
  dimension: team {
    type: string
    sql: ${TABLE}.team ;;
  }

  ## add in measures make sure all khm are in catapult
  dimension: d_has_atc {
    type: number
    hidden: yes
    label: "ATC"
    sql: case when ${TABLE}.atc is null then 0 else 1 end;;
  }
  dimension: d_has_conversion_rate {
    type: number
    hidden: yes
    label: "Conversion Rate"
    sql: case when ${TABLE}.conversion_rate is null then 0 else 1 end;;
  }
  dimension: d_has_pages_per_unit {
    type: number
    hidden: yes
    label: "Pages per Unit"
    sql: case when ${TABLE}.pages_per_unit is null then 0 else 1 end;;
  }
  dimension: d_has_mean_visits {
    type: number
    hidden: yes
    label: "Visit Frequency"
    sql: case when ${TABLE}.mean_visits is null then 0 else 1 end;;
  }
  dimension: d_has_mean_osa_revenue {
    type: number
    hidden: yes
    label: "Mean OSA Revenue"
    sql: case when ${TABLE}.mean_osa_revenue is null then 0 else 1 end;;
  }
  dimension: d_has_mean_engaged_visit {
    type: number
    hidden: yes
    label: "Mean Engaged Visit"
    sql: case when ${TABLE}.mean_engaged_visit is null then 0 else 1 end;;
  }
  dimension: d_has_ads_acxv {
    type: number
    hidden: yes
    label: "Ads ACXV"
    sql: case when ${TABLE}.ads_acxv is null then 0 else 1 end;;
  }
  dimension: d_has_checkout_start {
    type: number
    hidden: yes
    label: "Checkout Start"
    sql: case when ${TABLE}.checkout_start is null then 0 else 1 end;;
  }
  dimension: d_has_winsorized_acxv {
    type: number
    hidden: yes
    label: "Winsorized ACXV"
    sql: case when ${TABLE}.winsorized_acxv is null then 0 else 1 end;;
  }
  dimension: d_has_bounces {
    type: number
    hidden: yes
    label: "Bounces"
    sql: case when ${TABLE}.bounces is null then 0 else 1 end;;
  }
  dimension: d_has_ads_cvr {
    type: number
    hidden: yes
    label: "Ads CVR"
    sql: case when ${TABLE}.ads_cvr is null then 0 else 1 end;;
  }
  dimension: d_has_opu {
    type: number
    hidden: yes
    label: "Orders Per User"
    sql: case when ${TABLE}.opu is null then 0 else 1 end;;
  }
  dimension: d_has_aov {
    type: number
    hidden: yes
    label: "AOV"
    sql: case when ${TABLE}.aov is null then 0 else 1 end;;
  }
  dimension: d_has_mean_prolist_spend {
    type: number
    hidden: yes
    label: "Mean Prolist Spend"
    sql: case when ${TABLE}.mean_prolist_spend is null then 0 else 1 end;;
  }
  dimension: d_has_ocb {
    type: number
    hidden: yes
    label: "Orders per Converting Browser"
    sql: case when ${TABLE}.ocb is null then 0 else 1 end;;
  }
  measure: has_atc {
    type: number
    group_label: "KHM Tracker"
    label: "ATC"
    sql: max(case when ${TABLE}.atc is null then 0 else 1 end);;
  }
  measure: has_conversion_rate {
    type: number
    group_label: "KHM Tracker"
    label: "Conversion Rate"
    sql: max(case when ${TABLE}.conversion_rate is null then 0 else 1 end);;
  }
  measure: has_pages_per_unit {
    type: number
    group_label: "KHM Tracker"
    label: "Pages per Unit"
    sql: max(case when ${TABLE}.pages_per_unit is null then 0 else 1 end);;
  }
  measure: has_mean_visits {
    type: number
    group_label: "KHM Tracker"
    label: "Visit Frequency"
    sql: max(case when ${TABLE}.mean_visits is null then 0 else 1 end);;
  }
  measure: has_mean_osa_revenue {
    type: number
    group_label: "KHM Tracker"
    label: "Mean OSA Revenue"
    sql: max(case when ${TABLE}.mean_osa_revenue is null then 0 else 1 end);;
  }
  measure: has_mean_engaged_visit {
    type: number
    group_label: "KHM Tracker"
    label: "Mean Engaged Visit"
    sql: max(case when ${TABLE}.mean_engaged_visit is null then 0 else 1 end);;
  }
  measure: has_ads_acxv {
    type: number
    group_label: "KHM Tracker"
    label: "Ads ACXV"
    sql: max(case when ${TABLE}.ads_acxv is null then 0 else 1 end);;
  }
  measure: has_checkout_start {
    type: number
    group_label: "KHM Tracker"
    label: "Checkout Start"
    sql: max(case when ${TABLE}.checkout_start is null then 0 else 1 end);;
  }
  measure: has_winsorized_acxv {
    type: number
    group_label: "KHM Tracker"
    label: "Winsorized ACXV"
    sql: max(case when ${TABLE}.winsorized_acxv is null then 0 else 1 end);;
  }
  measure: has_bounces {
    type: number
    group_label: "KHM Tracker"
    label: "Bounces"
    sql: max(case when ${TABLE}.bounces is null then 0 else 1 end);;
  }
  measure: has_ads_cvr {
    type: number
    group_label: "KHM Tracker"
    label: "Ads CVR"
    sql: max(case when ${TABLE}.ads_cvr is null then 0 else 1 end);;
  }
  measure: has_opu {
    type: number
    group_label: "KHM Tracker"
    label: "Orders Per User"
    sql: max(case when ${TABLE}.opu is null then 0 else 1 end);;
  }
  measure: has_aov {
    type: number
    group_label: "KHM Tracker"
    label: "AOV"
    sql: max(case when ${TABLE}.aov is null then 0 else 1 end);;
  }
  measure: has_mean_prolist_spend {
    type: number
    group_label: "KHM Tracker"
    label: "Mean Prolist Spend"
    sql: max(case when ${TABLE}.mean_prolist_spend is null then 0 else 1 end);;
  }
  measure: has_ocb {
    type: number
    group_label: "KHM Tracker"
    label: "Orders per Converting Browser"
    sql: max(case when ${TABLE}.ocb is null then 0 else 1 end);;
  }
  # add together dimensions here so dont need to worry about aggregating aggregations 
  measure: has_khm {
    type: number
    group_label: "KHM Tracker"
    label: "Total Count"
    sql: sum(${d_has_atc}+${d_has_conversion_rate}+${d_has_pages_per_unit}+${d_has_mean_osa_revenue}+${d_has_mean_visits}+${d_has_mean_engaged_visit}+${d_has_ads_acxv}+${d_has_checkout_start}+${d_has_winsorized_acxv}+${d_has_bounces}+${d_has_ads_cvr}+${d_has_opu}+${d_has_aov}+${d_has_mean_prolist_spend}+${d_has_ocb});;
  }
  ##############measure
  measure: conversion_rate {
    type: average
    group_label: "Treatment"
    label: "Conversion Rate"
    sql: ${TABLE}.conversion_rate ;;
    value_format: "0.00%"
  }
  measure: control_conversion_rate {
    type: average
    group_label: "Control"
    label: "Control Conversion Rate"
    sql: ${TABLE}.control_conversion_rate ;;
    value_format: "0.00%"
  }

  measure: pct_change_conversion_rate {
    type: average
    group_label: "% Change"
    label: "Conversion Rate % Change"
    sql: ${TABLE}.pct_change_conversion_rate ;;
    value_format: "0.00%"
  }
  measure: pval_conversion_rate {
    type: average
    group_label: "P-Values"
    label: "Conversion Rate P-Value"
    sql: ${TABLE}.pval_conversion_rate ;;
    value_format: "0.00"
  }
  measure: mean_visits {
    type: average
    group_label: "Treatment"
    label: "Visit Frequency"
    sql: ${TABLE}.mean_visits ;;
    value_format: "0.00"
  }
  measure: control_mean_visits {
    type: average
    group_label: "Control"
    label: "Control Visit Frequency"
    sql: ${TABLE}.control_mean_visits ;;
    value_format: "0.00"
  }
  measure: pct_change_mean_visits {
    type: average
    group_label: "% Change"
    label: "Visit Frequency % Change"
    sql: ${TABLE}.pct_change_mean_visits ;;
    value_format: "0.00%"
  }
  measure: pval_mean_visits {
    type: average
    group_label: "P-Values"
    label: "Visit Frequency P-Value"
    sql: ${TABLE}.pval_mean_visits ;;
    value_format: "0.00"
  }
  measure: gms_per_unit {
    type: average
    group_label: "Treatment"
    label: "GMS per Unit"
    sql: ${TABLE}.gms_per_unit ;;
    value_format: "0.00"
  }
  measure: control_gms_per_unit {
    type: average
    group_label: "Control"
    label: "GMS per Unit"
    sql: ${TABLE}.control_gms_per_unit ;;
    value_format: "0.00"
  }
  measure: pct_change_gms_per_unit {
    type: average
    group_label: "% Change"
    label: "GMS per Unit % Change"
    sql: ${TABLE}.pct_change_gms_per_unit ;;
    value_format: "0.00%"
  }
  measure: pval_gms_per_unit {
    type: average
    group_label: "P-Values"
    label: "GMS per Unit P-Value"
    sql: ${TABLE}.pval_gms_per_unit ;;
    value_format: "0.00"
  }
  measure: error_pg_view {
    type: average
    group_label: "Treatment"
    label: "Error Pageview"
    sql: ${TABLE}.error_pg_view ;;
    value_format: "0.00%"
  }
  measure: control_percent_error_pg_view {
    type: average
    group_label: "Control"
    label: "Control % w/ Error Pageview"
    sql: ${TABLE}.control_percent_error_pg_view ;;
    value_format: "0.00%"
  }
  measure: pct_change_error_pg_view {
    type: average
    group_label: "% Change"
    label: "Error Pageview % Change"
    sql: ${TABLE}.pct_change_error_pg_view ;;
    value_format: "0.00%"
  }
  measure: pval_error_pg_view {
    type: average
    group_label: "P-Values"
    label: "Error Pageview P-Value"
    sql: ${TABLE}.pval_error_pg_view ;;
    value_format: "0.00"
  }
  measure: mean_engaged_visit {
    type: average
    group_label: "Treatment"
    label: "Engaged Visits"
    sql: ${TABLE}.mean_engaged_visit ;;
    value_format: "0.00"
  }
  measure: control_mean_engaged_visit {
    type: average
    group_label: "Control"
    label: "Engaged Visits"
    sql: ${TABLE}.control_mean_engaged_visit ;;
    value_format: "0.00"
  }
  measure: pct_change_mean_engaged_visit {
    type: average
    group_label: "% Change"
    label: "Engaged Visits % Change"
    sql: ${TABLE}.pct_change_mean_engaged_visit ;;
    value_format: "0.00%"
  }
  measure: pval_mean_engaged_visit {
    type: average
    group_label: "P-Values"
    label: "Engaged Visits P-Value"
    sql: ${TABLE}.pval_mean_engaged_visit ;;
    value_format: "0.00"
  }
  measure: bounces {
    type: average
    group_label: "Treatment"
    label: "Bounces"
    sql: ${TABLE}.bounces ;;
    value_format: "0.00"
  }
  measure: control_bounces{
    type: average
    group_label: "Control"
    label: "Control Bounces"
    sql: ${TABLE}.control_bounces;;
    value_format: "0.00"
  }
  measure: pct_change_bounces {
    type: average
    group_label: "% Change"
    label: "Bounces % Change"
    sql: ${TABLE}.pct_change_bounces ;;
    value_format: "0.00%"
  }
  measure: pval_bounces {
    type: average
    group_label: "P-Values"
    label: "Bounces P-Value"
    sql: ${TABLE}.pval_bounces ;;
    value_format: "0.00"
  }
  measure: pages_per_unit {
    type: average
    group_label: "Treatment"
    label: "Pages per Unit"
    sql: ${TABLE}.pages_per_unit ;;
    value_format: "0.00"
  }
  measure: control_pages_per_unit {
    type: average
    group_label: "Control"
    label: "Pages per Unit"
    sql: ${TABLE}.control_pages_per_unit ;;
    value_format: "0.00"
  }
  measure: pct_change_pages_per_unit{
    type: average
    group_label: "% Change"
    label: "Pages per Unit % Change"
    sql: ${TABLE}.pct_change_pages_per_unit;;
    value_format: "0.00%"
  }
  measure: pval_pages_per_unit{
    type: average
    group_label: "P-Values"
    label: "Pages per Unit P-Value"
    sql: ${TABLE}.pval_pages_per_unit;;
    value_format: "0.00"
  }
  measure: ads_cvr {
    type: average
    group_label: "Treatment"
    label: "Ads CR"
    sql: ${TABLE}.ads_cvr ;;
    value_format: "0.00"
  }
  measure: control_ads_cvr {
    type: average
    group_label: "Control"
    label: "Control Ads CVR"
    sql: ${TABLE}.control_ads_cvr ;;
    value_format: "0.00"
  }
  measure: pct_change_ads_cvr{
    type: average
    group_label: "% Change"
    label: "Ads CR % Change"
    sql: ${TABLE}.pct_change_ads_cvr;;
    value_format: "0.00%"
  }
  measure: pval_ads_cvr{
    type: average
    group_label: "P-Values"
    label: "Ads CR P-Value"
    sql: ${TABLE}.pval_ads_cvr;;
    value_format: "0.00"
  }
  measure: ads_acxv {
    type: average
    group_label: "Treatment"
    label: "Ads ACXV"
    sql: ${TABLE}.ads_acxv ;;
    value_format: "0.00"
  }
  measure: control_ads_acxv {
    type: average
    group_label: "Control"
    label: "Ads ACXV"
    sql: ${TABLE}.control_ads_acxv ;;
    value_format: "0.00"
  }
  measure: pct_change_ads_acxv{
    type: average
    group_label: "% Change"
    label: "Ads ACXV % Change"
    sql: ${TABLE}.pct_change_ads_acxv;;
    value_format: "0.00%"
  }
  measure: pval_ads_acxv{
    type: average
    group_label: "P-Values"
    label: "Ads ACXV P-Value"
    sql: ${TABLE}.pval_ads_acxv;;
    value_format: "0.00"
  }
  measure: winsorized_acxv {
    type: average
    group_label: "Treatment"
    label: "Winsorized ACXV"
    sql: ${TABLE}.winsorized_acxv ;;
    value_format: "0.00"
  }
  measure: control_winsorized_acxv {
    type: average
    group_label: "Control"
    label: "Control Ads ACXV"
    sql: ${TABLE}.control_winsorized_acxv ;;
    value_format: "0.00"
  }
  measure: pct_change_winsorized_acxv{
    type: average
    group_label: "% Change"
    label: "Winsorized ACXV % Change"
    sql: ${TABLE}.pct_change_winsorized_acxv;;
    value_format: "0.00%"
  }
  measure: pval_winsorized_acxv{
    type: average
    group_label: "P-Values"
    label: "Winsorized ACXV P-Value"
    sql: ${TABLE}.pval_winsorized_acxv;;
    value_format: "0.00"
  }
  measure: ocb {
    type: average
    group_label: "Treatment"
    label: "Orders per Converting Browser"
    sql: ${TABLE}.ocb ;;
    value_format: "0.00"
  }
  measure: control_ocb {
    type: average
    group_label: "Control"
    label: "Order per Converting Browser"
    sql: ${TABLE}.control_ocb ;;
    value_format: "0.00"
  }
  measure: pct_change_ocb {
    type: average
    group_label: "% Change"
    label: "Orders per Converting Browser % Change"
    sql: ${TABLE}.pct_change_ocb;;
    value_format: "0.00%"
  }
  measure: pval_ocb {
    type: average
    group_label: "P-Values"
    label: "Orders per Converting Browser P-Value"
    sql: ${TABLE}.pval_ocb;;
    value_format: "0.00"
  }
  measure: opu {
    type: average
    group_label: "Treatment"
    label: "Orders per User"
    sql: ${TABLE}.opu ;;
    value_format: "0.00"
  }
  measure: control_opu {
    type: average
    group_label: "Control"
    label: "Control Order per User"
    sql: ${TABLE}.control_opu ;;
    value_format: "0.00"
  }
  measure: pct_change_opu {
    type: average
    group_label: "% Change"
    label: "Orders per User % Change"
    sql: ${TABLE}.pct_change_opu;;
    value_format: "0.00%"
  }
  measure: pval_opu {
    type: average
    group_label: "P-Values"
    label: "Orders per User P-Value"
    sql: ${TABLE}.pval_opu;;
    value_format: "0.00"
  }
  measure: atc {
    type: average
    group_label: "Treatment"
    label: "Add to Cart"
    sql: ${TABLE}.atc ;;
    value_format: "0.00"
  }
  measure: control_atc {
    type: average
    group_label: "Control"
    label: "Add to Cart"
    sql: ${TABLE}.control_atc ;;
    value_format: "0.00"
  }
  measure: pct_change_atc {
    type: average
    group_label: "% Change"
    label: "Add to Cart % Change"
    sql: ${TABLE}.pct_change_atc;;
    value_format: "0.00%"
  }
  measure: pval_atc {
    type: average
    group_label: "P-Values"
    label: "Add to Cart P-Value"
    sql: ${TABLE}.pval_atc;;
    value_format: "0.00"
  }
  measure: checkout_start {
    type: average
    group_label: "Treatment"
    label: "Checkout Start"
    sql: ${TABLE}.checkout_start ;;
    value_format: "0.00"
  }
  measure: control_checkout_cart {
    type: average
    group_label: "Control"
    label: "Control Checkout Start"
    sql: ${TABLE}.control_checkout_cart ;;
    value_format: "0.00"
  }
  measure: pct_change_checkout_start {
    type: average
    group_label: "% Change"
    label: "Checkout Start % Change"
    sql: ${TABLE}.pct_change_checkout_start;;
    value_format: "0.00%"
  }
  measure: pval_checkout_start {
    type: average
    group_label: "P-Values"
    label: "Checkout Start P-Value"
    sql: ${TABLE}.pval_checkout_start;;
    value_format: "0.00"
  }
  measure: aov {
    type: average
    group_label: "Treatment"
    label: "AOV"
    sql: ${TABLE}.aov ;;
    value_format: "0.00"
  }
  measure: control_aov {
    type: average
    group_label: "Control"
    label: "AOV"
    sql: ${TABLE}.control_aov ;;
    value_format: "0.00"
  }
  measure: pct_change_aov {
    type: average
    group_label: "% Change"
    label: "AOV % Change"
    sql: ${TABLE}.pct_change_aov;;
    value_format: "0.00%"
  }
  measure: pval_aov {
    type: average
    group_label: "P-Values"
    label: "AOV P-Value"
    sql: ${TABLE}.pval_aov;;
    value_format: "0.00"
  }
  measure: mean_prolist_spend {
    type: average
    group_label: "Treatment"
    label: "Mean Prolist Spend"
    sql: ${TABLE}.mean_prolist_spend ;;
    value_format: "0.00"
  }
  measure: control_mean_prolist_spend {
    type: average
    group_label: "Control"
    label: "Control Mean Prolist Spend"
    sql: ${TABLE}.control_mean_prolist_spend ;;
    value_format: "0.00"
  }
  measure: pct_change_mean_prolist_spend {
    type: average
    group_label: "% Change"
    label: "Mean Prolist Spend % Change"
    sql: ${TABLE}.pct_change_mean_prolist_spend;;
    value_format: "0.00%"
  }
  measure: pval_mean_prolist_spend {
    type: average
    group_label: "P-Values"
    label: "Mean Prolist Spend P-Value"
    sql: ${TABLE}.pval_mean_prolist_spend;;
    value_format: "0.00"
  }
  measure: mean_osa_revenue {
    type: average
    group_label: "Treatment"
    label: "Mean OSA Spend"
    sql: ${TABLE}.mean_osa_revenue ;;
    value_format: "0.00"
  }
  measure: control_mean_osa_revenue {
    type: average
    group_label: "Control"
    label: "Mean OSA Revenue"
    sql: ${TABLE}.control_mean_osa_revenue ;;
    value_format: "0.00"
  }
  measure: pct_change_mean_osa_revenue {
    type: average
    group_label: "% Change"
    label: "Mean OSA Spend % Change"
    sql: ${TABLE}.pct_change_mean_osa_revenue;;
    value_format: "0.00%"
  }
  measure: pval_mean_osa_revenue {
    type: average
    group_label: "P-Values"
    label: "Mean OSA Spend P-Value"
    sql: ${TABLE}.pval_mean_osa_revenue;;
    value_format: "0.00"
  }
  measure: pct_homescreen_exit {
    type: average
    group_label: "Treatment"
    label: "% Homescreen Exit "
    sql: ${TABLE}.pct_homescreen_exit ;;
    value_format: "0.00%"
  }
  measure: pct_change_pct_homescreen_exit {
    type: average
    group_label: "% Change"
    label: "% Homescreen Exit % Change"
    sql: ${TABLE}.pct_change_pct_homescreen_exit;;
    value_format: "0.00%"
  }
  measure: control_pct_homescreen_exit {
    type: average
    group_label: "Control"
    label: "Control % Homescreen Exits"
    sql: ${TABLE}.control_pct_homescreen_exit ;;
    value_format: "0.00%"
  }
  measure: pval_pct_homescreen_exit {
    type: average
    group_label: "P-Values"
    label: "% Homescreen Exit P-Value"
    sql: ${TABLE}.pval_pct_homescreen_exit;;
    value_format: "0.00"
  }
  measure: pct_homescreen_clickthrough {
    type: average
    group_label: "Treatment"
    label: "% Homescreen Clickthrough"
    sql: ${TABLE}.pct_homescreen_clickthrough ;;
    value_format: "0.00%"
  }
  measure: pct_change_pct_homescreen_clickthrough {
    type: average
    group_label: "% Change"
    label: "% Homescreen Clickthrough % Change"
    sql: ${TABLE}.pct_change_pct_homescreen_clickthrough;;
    value_format: "0.00%"
  }
  measure: control_pct_homescreen_clickthrough {
    type: average
    group_label: "Control"
    label: "Control % Homescreen Clickthrough"
    sql: ${TABLE}.control_pct_homescreen_clickthrough ;;
    value_format: "0.00%"
  }
  measure: pval_pct_homescreen_clickthrough {
    type: average
    group_label: "P-Values"
    label: "% Homescreen Clickthrough P-Value"
    sql: ${TABLE}.pval_pct_homescreen_clickthrough;;
    value_format: "0.00"
  }
  measure: pct_w_engagement_with_collected_content {
    type: average
    group_label: "Treatment"
    label: "% w/ Engagement + Collected Content"
    sql: ${TABLE}.pct_w_engagement_with_collected_content ;;
    value_format: "0.00%"
  }
  measure: control_pct_w_engagement_with_collected_content {
    type: average
    group_label: "Control"
    label: "Control % w/ Engagement + Collected Content"
    sql: ${TABLE}.control_pct_w_engagement_with_collected_content ;;
    value_format: "0.00%"
  }
  measure: pct_change_pct_w_engagement_with_collected_content {
    type: average
    group_label: "% Change"
    label: "% w/ Engagement + Collected Content % Change"
    sql: ${TABLE}.pct_change_pct_w_engagement_with_collected_content;;
    value_format: "0.00%"
  }
  measure: pval_pct_w_engagement_with_collected_content {
    type: average
    group_label: "P-Values"
    label: "% w/ Engagement + Collected Content P-Value"
    sql: ${TABLE}.pval_pct_w_engagement_with_collected_content;;
    value_format: "0.00%"
  }
}
