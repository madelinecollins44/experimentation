 create or replace table etsy-data-warehouse-dev.madelinecollins.active_experiment_summary as (
 --grab all active experiments 
  WITH all_experiments as (
  SELECT
  DISTINCT l.launch_id,
  date(boundary_start_ts) AS start_date,
  _date AS last_run_date,
    case  
    when bucketing_id_type = 1 then 'Browser'
    else 'User'
    end as bucketing_type 
FROM
  `etsy-data-warehouse-prod.catapult_unified.experiment` AS e
INNER JOIN
  `etsy-data-warehouse-prod.etsy_atlas.catapult_experiment_boundaries` AS b
    ON UNIX_SECONDS(e.boundary_start_ts) = b.start_epoch
    AND e.experiment_id = b.config_flag
INNER JOIN
  `etsy-data-warehouse-prod.etsy_atlas.catapult_launches` AS l ON l.launch_id = b.launch_id
WHERE _date = CURRENT_DATE()-1
  AND l.state = 1 -- Currently running
  AND delete_date IS NULL -- Remove deleted experiments
)
-- pull out desired metrics 
, metrics_list as (
select  
  ae.launch_id
  , ae.bucketing_type
  , _date as last_run_date
  , ae.start_date
  , metric_variant_name
  , metric_display_name
  , metric_id
  , metric_value_control
  , metric_value_treatment
  , relative_change
  , is_significant
  , p_value
  , row_number() over(partition by ae.launch_id, metric_variant_name, boundary_start_sec,_date ,lower(metric_display_name) order by length(metric_stat_methodology) desc) as metric_rnk
from all_experiments ae
inner join `etsy-data-warehouse-prod.catapult.results_metric_day` rmd
  on ae.launch_id=rmd.launch_id
  and ae.last_run_date=rmd._date -- join on most recent date to get most recent data
qualify metric_rnk=1 -- prrioitized cuped values for pval
)
-- -- find control + treatment metrics 
, all_variants as (
  select
    launch_id
    , last_run_date
    , start_date
    , bucketing_type
    , metric_variant_name
    , max(case when lower(metric_display_name) = 'conversion rate' then metric_value_control else null end) as control_conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then metric_value_treatment else null end) as conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then relative_change else null end)/100 as pct_change_conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then p_value else null end) as pval_conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then is_significant else null end) as significance_conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_conversion_rate

--visit frequency
    , max(case when lower(metric_display_name) = 'mean visits' then metric_value_control else null end) as control_mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then metric_value_treatment else null end) as mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then relative_change else null end)/100 as pct_change_mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then p_value else null end) as pval_mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then is_significant else null end) as significance_mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_mean_visits

    , max(case when lower(metric_display_name) = 'gms per unit' then metric_value_control else null end) as control_gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then metric_value_treatment else null end) as gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then relative_change else null end)/100 as pct_change_gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then p_value else null end) as pval_gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then is_significant else null end) as significance_gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_gms_per_unit

    , max(case when lower(metric_display_name) = 'percent with error page view' then metric_value_control else null end) as control_percent_error_pg_view
    , max(case when lower(metric_display_name) = 'percent with error page view' then metric_value_treatment else null end) as percent_error_pg_view
    , max(case when lower(metric_display_name) = 'percent with error page view' then relative_change else null end)/100 as pct_change_percent_error_pg_view
    , max(case when lower(metric_display_name) = 'percent with error page view' then p_value else null end) as pval_percent_error_pg_view
    , max(case when lower(metric_display_name) = 'percent with error page view' then is_significant else null end) as significance_percent_error_pg_view
    , max(case when lower(metric_display_name) = 'percent with error page view' and relative_change > 0 then is_significant else null end) as positive_significance_percent_error_pg_view

    , max(case when lower(metric_display_name) = 'mean engaged_visit' then metric_value_control else null end) as control_mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then metric_value_treatment else null end) as mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then relative_change else null end)/100 as pct_change_mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then p_value else null end) as pval_mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then is_significant else null end) as significance_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_engaged_visit

    , max(case when lower(metric_display_name) = 'bounces' then metric_value_control else null end) as control_bounces
    , max(case when lower(metric_display_name) = 'bounces' then metric_value_treatment else null end) as bounces
    , max(case when lower(metric_display_name) = 'bounces' then relative_change else null end)/100 as pct_change_bounces
    , max(case when lower(metric_display_name) = 'bounces' then p_value else null end) as pval_bounces
    , max(case when lower(metric_display_name) = 'bounces' then is_significant else null end) as significance_bounces
    , max(case when lower(metric_display_name)= 'bounces' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_bounces

    -- , max(case when metric_display_name) = 'Mean total_winsorized_gms' then metric_value_control else null end) as control_mean_total_winsorized_gms
    -- , max(case when lower(metric_display_name) = 'Mean total_winsorized_gms' then metric_value_treatment else null end) as mean_total_winsorized_gms
    -- , max(case when lower(metric_display_name) = 'Mean total_winsorized_gms' then relative_change else null end)/100 as pct_mean_total_winsorized_gms
    -- , max(case when lower(metric_display_name) = 'Mean total_winsorized_gms' then p_value else null end) as pval_mean_total_winsorized_gms
    -- , max(case when lower(metric_display_name) = 'Mean total_winsorized_gms' then is_significant else null end) as significance_mean_total_winsorized_gms
    -- , max(case when lower(metric_display_name) = 'Mean total_winsorized_gms' and relative_change > 0 then is_significant else null end) as positive_significance_mean_total_winsorized_gms

    , max(case when lower(metric_display_name) = 'ads conversion rate' then metric_value_control else null end) as control_ads_cvr
    , max(case when lower(metric_display_name) = 'ads conversion rate' then metric_value_treatment else null end) as ads_cvr
    , max(case when lower(metric_display_name) = 'ads conversion rate' then relative_change else null end)/100 as pct_change_ads_cvr
    , max(case when lower(metric_display_name) = 'ads conversion rate' then p_value else null end) as pval_ads_cvr
    , max(case when lower(metric_display_name) = 'ads conversion rate' then is_significant else null end) as significance_ads_cvr
    , max(case when lower(metric_display_name) = 'ads conversion rate' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_ads_cvr

    , max(case when lower(metric_display_name) = 'ads winsorized acvv ($100)' then metric_value_control else null end) as control_ads_acxv
    , max(case when lower(metric_display_name) = 'ads winsorized acvv ($100)' then metric_value_treatment else null end) as ads_acxv
    , max(case when lower(metric_display_name) = 'ads winsorized acvv ($100)' then relative_change else null end)/100 as pct_change_ads_acxv
    , max(case when lower(metric_display_name) = 'ads winsorized acvv ($100)' then p_value else null end) as pval_ads_acxv
    , max(case when lower(metric_display_name) = 'ads winsorized acvv ($100)' then is_significant else null end) as significance_ads_acxv
    , max(case when lower(metric_display_name) = 'ads winsorized acvv ($100)' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_ads_acxv

    , max(case when lower(metric_display_name) = 'winsorized ac*v' then metric_value_control else null end) as control_winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then metric_value_treatment else null end) as winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then relative_change else null end)/100 as pct_change_winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then p_value else null end) as pval_winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then is_significant else null end) as significance_winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_winsorized_acxv

    -- , max(case when lower(metric_display_name) in ('Percent with Listing View', 'Percent with listing view') then metric_value_control else null end) as control_pct_listing_view
    -- , max(case when lower(metric_display_name) in ('Percent with Listing View', 'Percent with listing view')  then metric_value_treatment else null end) as pct_listing_view
    -- , max(case when lower(metric_display_name) in ('Percent with Listing View', 'Percent with listing view')  then relative_change else null end)/100 as pct_change_pct_listing_view
    -- , max(case when lower(metric_display_name) in ('Percent with Listing View', 'Percent with listing view')  then p_value else null end) as pval_pct_listing_view
    -- , max(case when lower(metric_display_name) in ('Percent with Listing View', 'Percent with listing view')  then is_significant else null end) as significance_pct_listing_view

    , max(case when lower(metric_display_name) = 'orders per converting browser (ocb)' then metric_value_control else null end) as control_ocb
    , max(case when lower(metric_display_name) = 'orders per converting browser (ocb)' then metric_value_treatment else null end) as ocb
    , max(case when lower(metric_display_name) = 'orders per converting browser (ocb)'  then relative_change else null end)/100 as pct_change_ocb
    , max(case when lower(metric_display_name) = 'orders per converting browser (ocb)'  then p_value else null end) as pval_ocb
    , max(case when lower(metric_display_name) = 'orders per converting browser (ocb)' then is_significant else null end) as significance_ocb
    , max(case when lower(metric_display_name) = 'orders per converting browser (ocb)' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_ocb

--purchase frequency
----should i use total 
    , max(case when lower(metric_display_name) = 'total orders per unit' then metric_value_control else null end) as control_opu
    , max(case when lower(metric_display_name) = 'total orders per unit' then metric_value_treatment else null end) as opu
    , max(case when lower(metric_display_name) = 'total orders per unit'then relative_change else null end)/100 as pct_change_opu
    , max(case when lower(metric_display_name) = 'total orders per unit' then p_value else null end) as pval_opu
    , max(case when lower(metric_display_name) = 'total orders per unit' then is_significant else null end) as significance_opu
    , max(case when lower(metric_display_name) = 'total orders per unit' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_opu

    , max(case when lower(metric_display_name) = 'percent with add to cart' then metric_value_control else null end) as control_atc
    , max(case when lower(metric_display_name) = 'percent with add to cart' then metric_value_treatment else null end) as atc
    , max(case when lower(metric_display_name) = 'percent with add to cart' then relative_change else null end)/100 as pct_change_atc
    , max(case when lower(metric_display_name) = 'percent with add to cart'then p_value else null end) as pval_atc
    , max(case when lower(metric_display_name) = 'percent with add to cart' then is_significant else null end) as significance_atc
    , max(case when lower(metric_display_name) = 'percent with add to cart'and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_atc

    , max(case when lower(metric_display_name) = 'percent with checkout_start' then metric_value_control else null end) as control_checkout_start
    , max(case when lower(metric_display_name) = 'percent with checkout_start' then metric_value_treatment else null end) as checkout_start
    , max(case when lower(metric_display_name) = 'percent with checkout_start' then relative_change else null end)/100 as pct_change_checkout_start
    , max(case when lower(metric_display_name) = 'percent with checkout_start' then p_value else null end) as pval_checkout_start
    , max(case when lower(metric_display_name) = 'percent with checkout_start' then is_significant else null end) as significance_checkout_start
    , max(case when lower(metric_display_name) = 'percent with checkout_start' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_checkout_start

    , max(case when lower(metric_display_name) = 'winsorized aov' then metric_value_control else null end) as control_aov
    , max(case when lower(metric_display_name) = 'winsorized aov' then metric_value_treatment else null end) as aov
    , max(case when lower(metric_display_name) = 'winsorized aov' then relative_change else null end)/100 as pct_change_aov
    , max(case when lower(metric_display_name) = 'winsorized aov' then p_value else null end) as pval_aov  
    , max(case when lower(metric_display_name) = 'winsorized aov' then is_significant else null end) as significance_aov
    , max(case when lower(metric_display_name) = 'winsorized aov' and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_aov

    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then metric_value_control else null end) as control_mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then metric_value_treatment else null end) as mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then relative_change else null end)/100 as pct_change_mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then p_value else null end) as pval_mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then is_significant else null end) as significance_mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_mean_prolist_spend

    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') then metric_value_control else null end) as control_mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') then metric_value_treatment else null end) as mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') then relative_change else null end)/100 as pct_change_mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue')  then p_value else null end) as pval_mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue')  then is_significant else null end) as significance_mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and relative_change > 0 and is_significant = true then 1 else 0 end) as positive_significance_mean_osa_revenue
    , row_number() over (partition by launch_id order by max(case when lower(metric_display_name) = 'conversion rate' then p_value else null end)) AS treatment_rank
  from metrics_list
where 
  1=1
  and metric_variant_name != 'off' --removed control as a metric_variant_name, but control metrics will still be there 
  group by all 
)
, off_gms AS (
  SELECT
      exp_off_key_metrics.experiment_id,
      exp_off_key_metrics.off_gms,
      exp_off_key_metrics.off_prolist_spend * exp_off_key_metrics.off_browsers / 100 AS off_prolist_spend
    FROM
      `etsy-data-warehouse-prod`.catapult.exp_off_key_metrics
    WHERE exp_off_key_metrics.segmentation = 'any'
     AND exp_off_key_metrics.run_date IN (
      SELECT
          max(exp_on_key_metrics_1.run_date)
        FROM
          `etsy-data-warehouse-prod`.catapult.exp_on_key_metrics AS exp_on_key_metrics_1)
  ORDER BY
    1
)
, exp_summary AS (
  SELECT
      -- a.experiment_id,
      a.launch_id,
      s.team,
      s.name,
      s.initiative,
      a.bucketing_type,
      count(distinct a.metric_variant_name) as treatments_per_experiment,
      --s.enabling_teams,
      s.launch_group as group_name,
      s.outcome,
      s.hypothesis,
      s.launch_percentage,
      a.start_date,
      a.last_run_date,
      date_diff(a.last_run_date, a.start_date, DAY) + 1 AS days_running,
      o.off_gms,
      o.off_prolist_spend,
      ----sig changes
      max(a.significance_conversion_rate) AS significance_conversion_rate,
      max(a.significance_mean_visits) AS significance_mean_visits,
      max(a.significance_gms_per_unit) AS significance_gms_per_unit,
      max(a.significance_percent_error_pg_view) AS significance_percent_error_pg_view,
      max(a.significance_engaged_visit) AS significance_engaged_visit,
      max(a.significance_bounces) AS significance_bounces,
      max(a.significance_ads_cvr) AS significance_ads_cvr,
      max(a.significance_winsorized_acxv) AS significance_winsorized_acxv,
      max(a.significance_ocb) AS significance_ocb,
      max(a.significance_opu) AS significance_opu,
      max(a.significance_atc) AS significance_atc,
      max(a.significance_checkout_start) AS significance_checkout_start,
      max(a.significance_aov) AS significance_aov,
      max(a.significance_mean_prolist_spend) AS significance_mean_prolist_spend,
      max(a.significance_mean_osa_revenue) AS significance_mean_osa_revenue,
      --- positive changes
      max(a.positive_significance_conversion_rate) AS positive_significance_conversion_rate,
      max(a.positive_significance_mean_visits) AS positive_significance_mean_visits,
      max(a.positive_significance_gms_per_unit) AS positive_significance_gms_per_unit,
      max(a.positive_significance_percent_error_pg_view) AS positive_significance_percent_error_pg_view,
      max(a.positive_significance_engaged_visit) AS positive_significance_engaged_visit,
      max(a.positive_significance_bounces) AS positive_significance_bounces,
      max(a.positive_significance_ads_cvr) AS positive_significance_ads_cvr,
      max(a.positive_significance_winsorized_acxv) AS positive_significance_winsorized_acxv,
      max(a.positive_significance_ocb) AS positive_significance_ocb,
      max(a.positive_significance_opu) AS positive_significance_opu,
      max(a.positive_significance_atc) AS positive_significance_atc,
      max(a.positive_significance_checkout_start) AS positive_significance_checkout_start,
      max(a.positive_significance_aov) AS positive_significance_aov,
      max(a.positive_significance_mean_prolist_spend) AS positive_significance_mean_prolist_spend,
      max(a.positive_significance_mean_osa_revenue) AS positive_significance_mean_osa_revenue,
     --cr
      control_conversion_rate,
      conversion_rate,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_conversion_rate
      END) AS cr_change_1,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_conversion_rate
      END) AS cr_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_conversion_rate
      END) AS cr_change_2,
            max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_conversion_rate
      END) AS cr_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_conversion_rate
      END) AS cr_change_3,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_conversion_rate
      END) AS cr_p_value_3,
    --visit frequency metrics
    control_mean_visits,
    mean_visits,
    max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_mean_visits
      END) AS visit_freq_p_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_mean_visits
      END) AS visit_freq_p_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_mean_visits
      END) AS visit_freq_p_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_mean_visits
      END) AS visit_freq_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_mean_visits
      END) AS visit_freq_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_mean_visits
      END) AS visit_freq_p_value_3,
      --gms per unit metrics
      control_gms_per_unit,
      gms_per_unit,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_gms_per_unit
      END) AS gms_per_unit_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_gms_per_unit
      END) AS gms_per_unit_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_gms_per_unit
      END) AS gms_per_unit_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_gms_per_unit
      END) AS gms_per_unit_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_gms_per_unit
      END) AS gms_per_unit_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_gms_per_unit
      END) AS gms_per_unit_p_value_3,
  -- error page
      control_percent_error_pg_view,
      percent_error_pg_view,
        max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_percent_error_pg_view
      END) AS error_pg_view_change_1,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_percent_error_pg_view
      END) AS error_pg_view_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_percent_error_pg_view
      END) AS error_pg_view_change_2,
            max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_percent_error_pg_view
      END) AS error_pg_view_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_percent_error_pg_view
      END) AS error_pg_view_change_3,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_percent_error_pg_view
      END) AS error_pg_view_p_value_3,
  -- enaged visits
   control_mean_engaged_visit,
   mean_engaged_visit,
  max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_mean_engaged_visit
      END) AS engaged_visits_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_mean_engaged_visit
      END) AS engaged_visits_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_mean_engaged_visit
      END) AS engaged_visits_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_mean_engaged_visit
      END) AS engaged_visits_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_mean_engaged_visit
      END) AS engaged_visits_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_mean_engaged_visit
      END) AS engaged_visits_p_value_3,
  -- bounces
  control_bounces,
  bounces,
   max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_bounces
      END) AS bounces_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_bounces
      END) AS bounces_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_bounces
      END) AS bounces_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_bounces
      END) AS bounces_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_bounces
      END) AS bounces_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_bounces
      END) AS bounces_p_value_3,
  -- ads cr
    control_ads_cvr,
    ads_cvr,
    max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_ads_cvr
      END) AS ads_cvr_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_ads_cvr
      END) AS ads_cvr_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_ads_cvr
      END) AS ads_cvr_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_ads_cvr
      END) AS ads_cvr_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_ads_cvr
      END) AS ads_cvr_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_ads_cvr
      END) AS ads_cvr_p_value_3,
  -- ads acvv
  control_ads_acxv, 
  ads_acxv,
    max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_ads_acxv
      END) AS ads_acxv_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_ads_acxv
      END) AS ads_acxv_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_ads_acxv
      END) AS ads_acxv_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_ads_acxv
      END) AS ads_acxv_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_ads_acxv
      END) AS ads_acxv_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_ads_acxv
      END) AS ads_acxv_p_value_3,
  -- wins acbv
    control_winsorized_acxv, 
  winsorized_acxv,
        max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_winsorized_acxv
      END) AS winsorized_acxv_change_1,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_winsorized_acxv
      END) AS pval_winsorized_acxv_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_winsorized_acxv
      END) AS winsorized_acxv_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_winsorized_acxv
      END) AS winsorized_acxv_change_3,
  --ocb
  control_ocb, 
  ocb,
   max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_ocb
      END) AS ocb_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_ocb
      END) AS ocb_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_ocb
      END) AS ocb_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_ocb
      END) AS ocb_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_ocb
      END) AS ocb_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_ocb
      END) AS ocb_p_value_3,
  --opu
  control_opu, 
  opu,
  max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_opu
      END) AS purchase_freq_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_opu
      END) AS purchase_freq_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_opu
      END) AS purchase_freq_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_opu
      END) AS purchase_freq_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_opu
      END) AS purchase_freq_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_opu
      END) AS purchase_freq_p_value_3,
  --atc
  control_atc, 
  atc,
   max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_atc
      END) AS atc_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_atc
      END) AS atc_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_atc
      END) AS atc_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_atc
      END) AS atc_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_atc
      END) AS atc_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_atc
      END) AS atc_p_value_3,
  --checkout
  control_checkout_start,  
  checkout_start,
  max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_checkout_start
      END) AS checkout_start_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_checkout_start
      END) AS checkout_start_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_checkout_start
      END) AS checkout_start_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_checkout_start
      END) AS checkout_start_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_checkout_start
      END) AS checkout_start_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_checkout_start
      END) AS checkout_start_p_value_3,
  --aov
  control_aov, 
  aov,
  max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_aov
      END) AS winsorized_aov_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_aov
      END) AS winsorized_aov_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_aov
      END) AS winsorized_aov_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_aov
      END) AS winsorized_aov_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_aov
      END) AS winsorized_aov_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_aov
      END) AS winsorized_aov_p_value_3,
  --prolist
  control_mean_prolist_spend, 
  mean_prolist_spend,
        max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_mean_prolist_spend
      END) AS mean_prolist_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_mean_prolist_spend
      END) AS mean_prolist_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_mean_prolist_spend
      END) AS mean_prolist_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_mean_prolist_spend
      END) AS prolist_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_mean_prolist_spend
      END) AS prolist_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_mean_prolist_spend
      END) AS prolist_p_value_3,
  --osa 
  control_mean_osa_revenue, 
  mean_osa_revenue,
   max(CASE
        WHEN a.treatment_rank = 1 THEN a.pct_change_mean_osa_revenue
      END) AS osa_revenue_attribution_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pct_change_mean_osa_revenue
      END) AS osa_revenue_attribution_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pct_change_mean_osa_revenue
      END) AS osa_revenue_attribution_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.pval_mean_osa_revenue
      END) AS osa_revenue_attribution_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.pval_mean_osa_revenue
      END) AS osa_revenue_attribution_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.pval_mean_osa_revenue
      END) AS osa_revenue_attribution_p_value_3
    FROM
      all_variants
       AS a
      LEFT JOIN off_gms AS o ON a.launch_id = o.experiment_id
      INNER JOIN `etsy-data-warehouse-prod`.etsy_atlas.catapult_launches AS s ON a.launch_id = s.launch_id
    GROUP BY all
), plats as (
-- This CTE gets the platform for each experiment (launch_id)
select 
  distinct launch_id
  , update_date
  , name as platform
  , dense_rank() over(partition by launch_id  order by update_date desc) AS row_num
FROM `etsy-data-warehouse-prod.etsy_atlas.catapult_launches_expected_platforms`
qualify 
  row_num = 1
)
, plats_agg as (
select 
  launch_id
  , STRING_AGG(platform ORDER BY platform) as platform
from plats
group by all
)
, experiment_pages_ran_on AS (
-- This CTE gets the pages that each experiment is run on 
SELECT  
  DISTINCT launch_id
  , update_date
  , row_number() over(partition by launch_id  order by update_date desc) as row_num
  , sum(case when name = "Unavailable Listing Page" then 1 else 0 end) as unavailable_listing
  , sum(case when name = "Category Page" then 1 else 0 end) as category
  , sum(case when name = "Checkout Page" then 1 else 0 end) as checkout
  , sum(case when name = "Cart" then 1 else 0 end) as cart
  , sum(case when name = "Other" then 1 else 0 end) as other 
  , sum(case when name = "Listing" then 1 else 0 end) as listing
  , sum(case when name = "Market" then 1 else 0 end) as market
  , sum(case when name = "Sitewide" then 1 else 0 end) as sitewide
  , sum(case when name = "Shop Home" then 1 else 0 end) as shop_home
  , sum(case when name = "Home" then 1 else 0 end) as home
  , sum(case when name = "Sold Out Listing Page" then 1 else 0 end) as sold_out_listing
  , sum(case when name = "Search" then 1 else 0 end) as _search 
from `etsy-data-warehouse-prod.etsy_atlas.catapult_launches_expected_pages`
where
  1=1
group by all
qualify row_num = 1
)
, exp_coverage as (
-- This CTE gets the coverage %'s for each experiment. It should match up with what's shown in the catapult page
select
  launch_id
  , coverage_name
  , date(timestamp_seconds(boundary_start_sec)) as start_date
  , date(timestamp_seconds(boundary_end_sec)) as end_date
  , dense_rank() over (partition by launch_id, date(timestamp_seconds(boundary_start_sec)) order by _date desc) as date_rank
  , cast(coverage_value/100 as float64) as coverage_value
from `etsy-data-warehouse-prod.catapult.results_coverage_day` 
qualify date_rank=1
)
select
  es.*,
  platform
from exp_summary es
left join plats_agg pa using (launch_id)
-- left join experiment_pages_ran_on ep
--   on pa.launch_id=ep.launch_id
 );
