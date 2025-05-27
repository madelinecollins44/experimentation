--owner: madelinecollins@etsy.com
--owner_team: product-asf@etsy.com
--description: track khm for all active experiments in catapult

create or replace table etsy-data-warehouse-prod.rollups.active_experiment_summary as (
 
 --grab all active experiments 
WITH all_experiments as (
  SELECT
  DISTINCT l.launch_id,
  e.experiment_id as config_flag,
  triggered_by_experiment_certification as has_certification_run,
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
-- This CTE gets the coverage %'s for each experiment. It should match up with what's shown in the catapult page
, exp_coverage as (
select
  launch_id
  , coverage_name
  , date(timestamp_seconds(boundary_start_sec)) as start_date
  , date(timestamp_seconds(boundary_end_sec)) as end_date
  , dense_rank() over (partition by launch_id, date(timestamp_seconds(boundary_start_sec)) order by _date desc) as date_rank
  , cast(coverage_value/100 as float64) as coverage_value
from `etsy-data-warehouse-prod.catapult.results_coverage_day` 
inner join all_experiments using (launch_id)
where segmentation = "any"
  and segment = "all"
qualify date_rank=1
)
, exp_coverage_agg as (
select
  launch_id
  , end_date
  , row_number () over (partition by launch_id order by end_date desc) as most_recent_day
  , max(case when coverage_name = 'GMS coverage' then coverage_value else null end) as gms_coverage
  , max(case when coverage_name = 'Traffic coverage' then coverage_value else null end) as traffic_coverage
  , max(case when coverage_name = 'Offsite Ads coverage' then coverage_value else null end) as osa_coverage
  , max(case when coverage_name = 'Prolist coverage' then coverage_value else null end) as prolist_coverage
from exp_coverage
group by all
qualify most_recent_day=1
)
  -- pull out desired metrics 
-----when inner joining, some experiments are dropped. those experiments are pes that have a current date range change so they will not hit the 100 day liimt 
, metrics_list as (
select  
  ae.launch_id
  , ae.has_certification_run
  , ae.config_flag
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
where segmentation = "any"
  and segment = "all"
qualify metric_rnk=1 -- prioritize cuped values for pval
)

  -- find control + treatment metrics 
, all_variants as (
  select
    launch_id
    , has_certification_run
    , config_flag
    , last_run_date
    , start_date
    , bucketing_type
    , metric_variant_name
    , max(case when lower(metric_display_name) = 'conversion rate' then metric_value_control else null end) as control_conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then metric_value_treatment else null end) as conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then relative_change else null end)/100 as pct_change_conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then p_value else null end) as pval_conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then is_significant else null end) as significance_conversion_rate
    --pages per unit
   , max(case when lower(metric_display_name) = 'pages per unit' then metric_value_control else null end) as control_pages_per_unit
    , max(case when lower(metric_display_name) = 'pages per unit' then metric_value_treatment else null end) as pages_per_unit
    , max(case when lower(metric_display_name) = 'pages per unit' then relative_change else null end)/100 as pct_change_pages_per_unit
    , max(case when lower(metric_display_name) = 'pages per unit' then p_value else null end) as pval_pages_per_unit
    , max(case when lower(metric_display_name) = 'pages per unit' then is_significant else null end) as significance_pages_per_unit
--visit frequency
    , max(case when lower(metric_display_name) = 'mean visits' then metric_value_control else null end) as control_mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then metric_value_treatment else null end) as mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then relative_change else null end)/100 as pct_change_mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then p_value else null end) as pval_mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then is_significant else null end) as significance_mean_visits

    , max(case when lower(metric_display_name) = 'gms per unit' then metric_value_control else null end) as control_gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then metric_value_treatment else null end) as gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then relative_change else null end)/100 as pct_change_gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then p_value else null end) as pval_gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then is_significant else null end) as significance_gms_per_unit

    , max(case when lower(metric_display_name) = 'percent with error page view' then metric_value_control else null end) as control_percent_error_pg_view
    , max(case when lower(metric_display_name) = 'percent with error page view' then metric_value_treatment else null end) as percent_error_pg_view
    , max(case when lower(metric_display_name) = 'percent with error page view' then relative_change else null end)/100 as pct_change_percent_error_pg_view
    , max(case when lower(metric_display_name) = 'percent with error page view' then p_value else null end) as pval_percent_error_pg_view
    , max(case when lower(metric_display_name) = 'percent with error page view' then is_significant else null end) as significance_percent_error_pg_view

    , max(case when lower(metric_display_name) = 'mean engaged_visit' then metric_value_control else null end) as control_mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then metric_value_treatment else null end) as mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then relative_change else null end)/100 as pct_change_mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then p_value else null end) as pval_mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then is_significant else null end) as significance_engaged_visit

    , max(case when lower(metric_display_name) = 'bounces' then metric_value_control else null end) as control_bounces
    , max(case when lower(metric_display_name) = 'bounces' then metric_value_treatment else null end) as bounces
    , max(case when lower(metric_display_name) = 'bounces' then relative_change else null end)/100 as pct_change_bounces
    , max(case when lower(metric_display_name) = 'bounces' then p_value else null end) as pval_bounces
    , max(case when lower(metric_display_name) = 'bounces' then is_significant else null end) as significance_bounces

    , max(case when lower(metric_display_name) = 'ads conversion rate' then metric_value_control else null end) as control_ads_cvr
    , max(case when lower(metric_display_name) = 'ads conversion rate' then metric_value_treatment else null end) as ads_cvr
    , max(case when lower(metric_display_name) = 'ads conversion rate' then relative_change else null end)/100 as pct_change_ads_cvr
    , max(case when lower(metric_display_name) = 'ads conversion rate' then p_value else null end) as pval_ads_cvr
    , max(case when lower(metric_display_name) = 'ads conversion rate' then is_significant else null end) as significance_ads_cvr

    , max(case when lower(metric_display_name) = 'ads winsorized ac*v ($100)' then metric_value_control else null end) as control_ads_acxv
    , max(case when lower(metric_display_name) = 'ads winsorized ac*v ($100)' then metric_value_treatment else null end) as ads_acxv
    , max(case when lower(metric_display_name) = 'ads winsorized ac*v ($100)' then relative_change else null end)/100 as pct_change_ads_acxv
    , max(case when lower(metric_display_name) = 'ads winsorized ac*v ($100)' then p_value else null end) as pval_ads_acxv
    , max(case when lower(metric_display_name) = 'ads winsorized ac*v ($100)' then is_significant else null end) as significance_ads_acxv

    , max(case when lower(metric_display_name) = 'winsorized ac*v' then metric_value_control else null end) as control_winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then metric_value_treatment else null end) as winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then relative_change else null end)/100 as pct_change_winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then p_value else null end) as pval_winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then is_significant else null end) as significance_winsorized_acxv

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

--purchase frequency
    , max(case when lower(metric_display_name) = 'orders per unit' then metric_value_control else null end) as control_opu
    , max(case when lower(metric_display_name) = 'orders per unit' then metric_value_treatment else null end) as opu
    , max(case when lower(metric_display_name) = 'orders per unit' then relative_change else null end)/100 as pct_change_opu
    , max(case when lower(metric_display_name) = 'orders per unit' then p_value else null end) as pval_opu
    , max(case when lower(metric_display_name) = 'orders per unit' then is_significant else null end) as significance_opu

    , max(case when lower(metric_display_name) = 'percent with add to cart' then metric_value_control else null end) as control_atc
    , max(case when lower(metric_display_name) = 'percent with add to cart' then metric_value_treatment else null end) as atc
    , max(case when lower(metric_display_name) = 'percent with add to cart' then relative_change else null end)/100 as pct_change_atc
    , max(case when lower(metric_display_name) = 'percent with add to cart'then p_value else null end) as pval_atc
    , max(case when lower(metric_display_name) = 'percent with add to cart' then is_significant else null end) as significance_atc

    , max(case when lower(metric_display_name) = 'percent with checkout_start' then metric_value_control else null end) as control_checkout_start
    , max(case when lower(metric_display_name) = 'percent with checkout_start' then metric_value_treatment else null end) as checkout_start
    , max(case when lower(metric_display_name) = 'percent with checkout_start' then relative_change else null end)/100 as pct_change_checkout_start
    , max(case when lower(metric_display_name) = 'percent with checkout_start' then p_value else null end) as pval_checkout_start
    , max(case when lower(metric_display_name) = 'percent with checkout_start' then is_significant else null end) as significance_checkout_start

    , max(case when lower(metric_display_name) = 'winsorized aov' then metric_value_control else null end) as control_aov
    , max(case when lower(metric_display_name) = 'winsorized aov' then metric_value_treatment else null end) as aov
    , max(case when lower(metric_display_name) = 'winsorized aov' then relative_change else null end)/100 as pct_change_aov
    , max(case when lower(metric_display_name) = 'winsorized aov' then p_value else null end) as pval_aov  
    , max(case when lower(metric_display_name) = 'winsorized aov' then is_significant else null end) as significance_aov

    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then metric_value_control else null end) as control_mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then metric_value_treatment else null end) as mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then relative_change else null end)/100 as pct_change_mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then p_value else null end) as pval_mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then is_significant else null end) as significance_mean_prolist_spend

    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') then metric_value_control else null end) as control_mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') then metric_value_treatment else null end) as mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') then relative_change else null end)/100 as pct_change_mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue')  then p_value else null end) as pval_mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue')  then is_significant else null end) as significance_mean_osa_revenue

    , max(case when lower(metric_display_name) in ('percent with homescreen_exit') then metric_value_control else null end) as control_pct_homescreen_exit
    , max(case when lower(metric_display_name) in ('percent with homescreen_exit') then metric_value_treatment else null end) as pct_homescreen_exit
    , max(case when lower(metric_display_name) in ('percent with homescreen_exit') then relative_change else null end)/100 as pct_change_pct_homescreen_exit
    , max(case when lower(metric_display_name) in ('percent with homescreen_exit') then p_value else null end) as pval_pct_homescreen_exit
    , max(case when lower(metric_display_name) in ('percent with homescreen_exit') then is_significant else null end) as significance_pct_homescreen_exit

    , max(case when lower(metric_display_name) in ('percent with homescreen_clickthrough') then metric_value_control else null end) as control_pct_homescreen_clickthrough
    , max(case when lower(metric_display_name) in ('percent with homescreen_clickthrough') then metric_value_treatment else null end) as pct_homescreen_clickthrough
    , max(case when lower(metric_display_name) in ('percent with homescreen_clickthrough') then relative_change else null end)/100 as pct_change_pct_homescreen_clickthrough
    , max(case when lower(metric_display_name) in ('percent with homescreen_clickthrough') then p_value else null end) as pval_pct_homescreen_clickthrough
    , max(case when lower(metric_display_name) in ('percent with homescreen_clickthrough') then is_significant else null end) as significance_pct_homescreen_clickthrough

    , max(case when lower(metric_display_name) in ('percent with engagement_with_collected_content') then metric_value_control else null end) as control_pct_w_engagement_with_collected_content
    , max(case when lower(metric_display_name) in ('percent with engagement_with_collected_content') then metric_value_treatment else null end) as pct_w_engagement_with_collected_content
    , max(case when lower(metric_display_name) in ('percent with engagement_with_collected_content') then relative_change else null end)/100 as pct_change_pct_w_engagement_with_collected_content
    , max(case when lower(metric_display_name) in ('percent with engagement_with_collected_content') then p_value else null end) as pval_pct_w_engagement_with_collected_content
    , max(case when lower(metric_display_name) in ('percent with engagement_with_collected_content') then is_significant else null end) as significance_pct_w_engagement_with_collected_content

    , row_number() over (partition by launch_id order by max(case when lower(metric_display_name) = 'conversion rate' then p_value else null end)) AS treatment_rank

  from metrics_list
where 
  1=1
  group by all
), off_gms AS (
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
      a.config_flag,
      a.launch_id,
      a.has_certification_run,
      s.team,
      s.name,
      s.initiative,
      b.auth_username as analyst,
      a.bucketing_type,
      a.metric_variant_name as name_of_treatment,
      count(distinct a.metric_variant_name) over (partition by a.launch_id) as number_of_treatments,
      --s.enabling_teams,
      s.launch_group as group_name,
      s.outcome,
      s.hypothesis,
      s.launch_percentage,
      a.start_date,
      a.last_run_date,
      date_diff(a.last_run_date, a.start_date, DAY) AS days_running,
      o.off_gms,
      o.off_prolist_spend,
      ----sig changes
      max(a.significance_conversion_rate) AS significance_conversion_rate,
      max(a.significance_mean_visits) AS significance_mean_visits,
      max(a.significance_gms_per_unit) AS significance_gms_per_unit,
      max(a.significance_pages_per_unit) AS significance_pages_per_unit,
      max(a.significance_percent_error_pg_view) AS significance_percent_error_pg_view,
      max(a.significance_engaged_visit) AS significance_engaged_visit,
      max(a.significance_bounces) AS significance_bounces,
      max(a.significance_ads_cvr) AS significance_ads_cvr,
      max(a.significance_winsorized_acxv) AS significance_winsorized_acxv,
      max(a.significance_ads_acxv) AS significance_ads_acxv,
      max(a.significance_ocb) AS significance_ocb,
      max(a.significance_opu) AS significance_opu,
      max(a.significance_atc) AS significance_atc,
      max(a.significance_checkout_start) AS significance_checkout_start,
      max(a.significance_aov) AS significance_aov,
      max(a.significance_mean_prolist_spend) AS significance_mean_prolist_spend,
      max(a.significance_mean_osa_revenue) AS significance_mean_osa_revenue,
      max(a.significance_pct_homescreen_exit) AS significance_pct_homescreen_exit,
      max(a.significance_pct_homescreen_clickthrough) AS significance_pct_homescreen_clickthrough,
      max(a.significance_pct_w_engagement_with_collected_content) AS significance_pct_w_engagement_with_collected_content,
     --cr
      control_conversion_rate,
      conversion_rate,
      pct_change_conversion_rate,
      pval_conversion_rate,
    --visit frequency metrics
    control_mean_visits,
    mean_visits,
    pct_change_mean_visits,
    pval_mean_visits,
      --gms per unit metrics
      control_gms_per_unit,
      gms_per_unit,
      pct_change_gms_per_unit,
      pval_gms_per_unit,
  -- error page
      control_percent_error_pg_view,
      percent_error_pg_view,
      pct_change_percent_error_pg_view,
      pval_percent_error_pg_view,
  -- enaged visits
   control_mean_engaged_visit,
   mean_engaged_visit,
   pct_change_mean_engaged_visit,
   pval_mean_engaged_visit,
  -- bounces
  control_bounces,
  bounces,
  pct_change_bounces,
  pval_bounces,
    --pages per unit       
      control_pages_per_unit,
     pages_per_unit,
     pct_change_pages_per_unit,
     pval_pages_per_unit,
  -- ads cr
    control_ads_cvr,
    ads_cvr,
    pct_change_ads_cvr,
    pval_ads_cvr,
  -- ads acvv
  control_ads_acxv, 
  ads_acxv,
  pct_change_ads_acxv,
  pval_ads_acxv,
  -- wins acbv
    control_winsorized_acxv, 
    winsorized_acxv,
    pct_change_winsorized_acxv,
    pval_winsorized_acxv,
  --ocb
  control_ocb, 
  ocb,
  pct_change_ocb,
  pval_ocb,
  --opu
  control_opu, 
  opu,
  pct_change_opu,
  pval_opu,
  --atc
  control_atc, 
  atc,
  pct_change_atc,
  pval_atc,
  --checkout
  control_checkout_start,  
  checkout_start,
  pct_change_checkout_start,
  pval_checkout_start,
  --aov
  control_aov, 
  aov,
  pct_change_aov,
  pval_aov,
  --prolist
  control_mean_prolist_spend, 
  mean_prolist_spend,
  pct_change_mean_prolist_spend,
  pval_mean_prolist_spend,
  --osa 
  control_mean_osa_revenue, 
  mean_osa_revenue,
  pct_change_mean_osa_revenue,
  pval_mean_osa_revenue
--homescreen exit
   , control_pct_homescreen_exit
    , pct_homescreen_exit
    , pct_change_pct_homescreen_exit
    , pval_pct_homescreen_exit
    --homescreen clickthrough
   , control_pct_homescreen_clickthrough
    , pct_homescreen_clickthrough
    , pct_change_pct_homescreen_clickthrough
    , pval_pct_homescreen_clickthrough
--engaged and collect
  , control_pct_w_engagement_with_collected_content
  , pct_w_engagement_with_collected_content
  , pct_change_pct_w_engagement_with_collected_content
  , pval_pct_w_engagement_with_collected_content
    FROM
      all_variants
       AS a
      LEFT JOIN off_gms AS o ON a.launch_id = o.experiment_id
      INNER JOIN `etsy-data-warehouse-prod`.etsy_atlas.catapult_launches AS s ON a.launch_id = s.launch_id
    left join etsy-data-warehouse-prod.etsy_aux.staff b on s.staff_id=b.id
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
  , sum(case when name = "Search" then 1 else 0 end) as search 
from `etsy-data-warehouse-prod.etsy_atlas.catapult_launches_expected_pages`
where
  1=1
group by  
  1,2
qualify
  row_num = 1
)
, alerts as (
select 
  feature_flag,
  max(case when alert_type in ('perf-web') then 1 else 0 end) as perf_web_alert,
  max(case when alert_type in ('streaming') then 1 else 0 end) as streaming_alert,
  max(case when alert_type in ('early-stopping') then 1 else 0 end) as early_stopping_alert,
  max(case when alert_type in ('prod') then 1 else 0 end) as prod_alert,
  max(case when alert_type in ('data-loss') then 1 else 0 end) as data_loss_alert,
  max(case when alert_type in ('bucketing-skew') then 1 else 0 end) as bucketing_skew_alert,
  max(case when alert_type in ('perf-native') then 1 else 0 end) as perf_native_alert,
  max(case when alert_type in ('segment-imbalance') then 1 else 0 end) as segment_imbalance_alert,
from
  `etsy-data-warehouse-prod.kafe.CatapultAlertService_AlertEvent` 
where 
  _date = (select max(_date) from `etsy-data-warehouse-prod.kafe.CatapultAlertService_AlertEvent`) 
group by all 
)
select
  es.*,
  platform
  , gms_coverage
  , traffic_coverage
  , osa_coverage
  , prolist_coverage
  -- bucketing info 
  , sum(case when bt.feature_flag is not null then 1 else 0 end) as alerts
  , perf_web_alert
  , streaming_alert
  , early_stopping_alert
  , prod_alert
  , data_loss_alert
  , bucketing_skew_alert
  , perf_native_alert
  , segment_imbalance_alert
  -- surface checks 
  , max(unavailable_listing) as unavailable_listing_page
  , max(category) as category_page
  , max(checkout) as checkout_page
  , max(cart) as cart_page
  , max(other) as other_page
  , max(listing) as listing_page
  , max(market) as market_page
  , max(sitewide) as sitewide_page
  , max(shop_home) as shop_home_page
  , max(home) as home_page
  , max(sold_out_listing) as sold_out_listing_page
  , max(search) as search_page 
from exp_summary es
left join plats_agg pa using (launch_id)
left join experiment_pages_ran_on pr using (launch_id)
left join exp_coverage_agg using (launch_id)
left join alerts bt
  on bt.feature_flag=es.config_flag
group by all
 );
