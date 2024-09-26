-----------------------------------------------------------------------------
--TAKE BASE FROM EXISTING ROLLUP, NEED TO ADD IN METRICS USING RESULTS_METRICS_DAY
-----------------------------------------------------------------------------
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
, off_gms AS (
  SELECT
      exp_off_key_metrics.experiment_id,
      exp_off_key_metrics.off_gms,
      exp_off_key_metrics.off_prolist_spend * exp_off_key_metrics.off_browsers / 100 AS off_prolist_spend
    FROM
      `etsy-data-warehouse-prod`.catapult.exp_off_key_metrics
    WHERE exp_off_key_metrics.segmentation = 'any'
     AND exp_off_key_metrics.run_date IN(
      SELECT
          max(exp_on_key_metrics_1.run_date)
        FROM
          `etsy-data-warehouse-prod`.catapult.exp_on_key_metrics AS exp_on_key_metrics_1
    )
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
      -- a.treatments_per_experiment,
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
    FROM
      all_experiments AS a
      LEFT JOIN off_gms AS o ON a.launch_id = o.experiment_id
      INNER JOIN `etsy-data-warehouse-prod`.etsy_atlas.catapult_launches AS s ON a.launch_id = s.launch_id
    GROUP BY all
)
, daily_gms AS (
  SELECT
      transactions_gms_by_trans.date,
      sum(transactions_gms_by_trans.trans_gms_gross) AS total_gms
    FROM
      `etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans
    WHERE transactions_gms_by_trans.date >= (
      SELECT
          min(exp_summary.start_date)
        FROM
          exp_summary
    )
    GROUP BY 1
  ORDER BY
    1
), daily_prolist_spend AS (
  SELECT
      prolist_daily_summary.date,
      prolist_daily_summary.spend AS total_prolist_spend
    FROM
      `etsy-data-warehouse-prod`.rollups.prolist_daily_summary
    WHERE prolist_daily_summary.date >= (
      SELECT
          min(exp_summary_0.start_date)
        FROM
          exp_summary AS exp_summary_0
    )
  ORDER BY
    1
), daily_denoms AS (
  SELECT
      g.date,
      g.total_gms,
      p.total_prolist_spend
    FROM
      daily_gms AS g
      INNER JOIN daily_prolist_spend AS p ON g.date = p.date
  ORDER BY
    1
), enabling_teams as (SELECT launch_id, STRING_AGG(DISTINCT `enabling_team_name` ORDER BY `enabling_team_name`) AS enabling_teams
        FROM  `etsy-data-warehouse-prod.etsy_atlas.catapult_launches_enabling_team`
        GROUP BY launch_id),
SELECT
    -- a_0.experiment_id,
    a_0.launch_id,
    a_0.bucketing_type,
    -- a_0.treatments_per_experiment,
    a_0.team,
    a_0.name,
    a_0.initiative,
    a_0.group_name,
    e.enabling_teams,
    a_0.outcome,
    a_0.hypothesis,
    a_0.launch_percentage,
    a_0.start_date,
    a_0.last_run_date,
    a_0.days_running,
  FROM
    exp_summary AS a_0
    INNER JOIN daily_denoms AS d ON d.date BETWEEN a_0.start_date AND a_0.last_run_date
    left join enabling_teams e on e.launch_id = a_0.launch_id
   GROUP BY all
ORDER BY
  days_running


-----------------------------------------------------------------------------
-- METRICS HERE
----------------------------------------------------------------------------

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
,metrics_list as (
select  
  ae.launch_id
  , _date
  , metric_variant_name
  , metric_display_name
  , metric_id
  , metric_value_control
  , metric_value_treatment
  , relative_change
  , p_value
from all_experiments ae
inner join `etsy-data-warehouse-prod.catapult.results_metric_day` rmd
  on ae.launch_id=rmd.launch_id
  and ae.last_run_date=rmd._date -- join on most recent date to get most recent data
)
, all_variants as (
  select
    launch_id
    , _date
    , metric_variant_name
    , max(case when lower(metric_display_name) = 'conversion rate' then metric_value_control else null end) as control_conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then metric_value_treatment else null end) as conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then relative_change else null end)/100 as pct_change_conversion_rate
    , max(case when lower(metric_display_name) = 'conversion rate' then p_value else null end) as pval_conversion_rate
    , max(case when lower(metric_display_name) = 'mean visits' then metric_value_control else null end) as control_mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then metric_value_treatment else null end) as mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then relative_change else null end)/100 as pct_change_mean_visits
    , max(case when lower(metric_display_name) = 'mean visits' then p_value else null end) as pval_mean_visits
    , max(case when lower(metric_display_name) = 'gms per unit' then metric_value_control else null end) as control_gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then metric_value_treatment else null end) as gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then relative_change else null end)/100 as pct_change_gms_per_unit
    , max(case when lower(metric_display_name) = 'gms per unit' then p_value else null end) as pval_gms_per_unit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then metric_value_control else null end) as control_mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then metric_value_treatment else null end) as mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then relative_change else null end)/100 as pct_change_mean_engaged_visit
    , max(case when lower(metric_display_name) = 'mean engaged_visit' then p_value else null end) as pval_mean_engaged_visit
    -- , max(case when lower(metric_display_name) = 'ads conversion rate' then metric_value_control else null end) as control_ads_cvr
    -- , max(case when lower(metric_display_name) = 'ads conversion rate' then metric_value_treatment else null end) as ads_cvr
    -- , max(case when lower(metric_display_name) = 'ads conversion rate' then relative_change else null end)/100 as pct_change_ads_cvr
    -- , max(case when lower(metric_display_name) = 'ads conversion rate' then p_value else null end) as pval_ads_cvr
    -- , max(case when lower(metric_display_name) = 'ads winsorized acvv ($100)' then metric_value_control else null end) as control_ads_acxv
    -- , max(case when lower(metric_display_name) = 'ads winsorized acvv ($100)' then metric_value_treatment else null end) as ads_acxv
    -- , max(case when lower(metric_display_name) = 'ads winsorized acvv ($100)' then relative_change else null end)/100 as pct_change_ads_acxv
    -- , max(case when lower(metric_display_name) = 'ads winsorized acvv ($100)' then p_value else null end) as pval_ads_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then metric_value_control else null end) as control_winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then metric_value_treatment else null end) as winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then relative_change else null end)/100 as pct_change_winsorized_acxv
    , max(case when lower(metric_display_name) = 'winsorized ac*v' then p_value else null end) as pval_winsorized_acxv
    , max(case when lower(metric_display_name) = 'orders per converting browser (ocb)' then metric_value_control else null end) as control_ocb
    , max(case when lower(metric_display_name) = 'orders per converting browser (ocb)' then metric_value_treatment else null end) as ocb
    , max(case when lower(metric_display_name) = 'orders per converting browser (ocb)' then relative_change else null end)/100 as pct_change_ocb
    , max(case when lower(metric_display_name) = 'orders per converting browser (ocb)' then p_value else null end) as pval_ocb
    , max(case when lower(metric_display_name) = 'total orders per unit' then metric_value_control else null end) as control_opu
    , max(case when lower(metric_display_name) = 'total orders per unit' then metric_value_treatment else null end) as opu
    , max(case when lower(metric_display_name) = 'total orders per unit' then relative_change else null end)/100 as pct_change_opu
    , max(case when lower(metric_display_name) = 'total orders per unit' then p_value else null end) as pval_opu
    , max(case when lower(metric_display_name) = 'winsorized aov' then metric_value_control else null end) as control_aov
    , max(case when lower(metric_display_name) = 'winsorized aov' then metric_value_treatment else null end) as aov
    , max(case when lower(metric_display_name) = 'winsorized aov' then relative_change else null end)/100 as pct_change_aov
    , max(case when lower(metric_display_name) = 'winsorized aov' then p_value else null end) as pval_aov  
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then metric_value_control else null end) as control_mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then metric_value_treatment else null end) as mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then relative_change else null end)/100 as pct_change_mean_prolist_spend
    , max(case when lower(metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') then p_value else null end) as pval_mean_prolist_spend
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') then metric_value_control else null end) as control_mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') then metric_value_treatment else null end) as mean_osa_revenue
    , max(case when lower(metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') then relative_change else null end)/100 as pct_change_mean_osa_revenue
  from metrics_list
  group by all 
)
select 
    *,
  count(metric_variant_name) over (partition by launch_id) as treatments_per_experiment,
  ((NUMERIC '0.05')/ (count(metric_variant_name) over (partition by launch_id))) as stat_sign_thresold,
  --conversion rate metrics
        CASE
          WHEN pval_conversion_rate <= ((NUMERIC '0.05')/ (count(metric_variant_name) over (partition by launch_id))) THEN 1
          ELSE 0
        END AS significant_cr_change,
              CASE
          WHEN pval_conversion_rate <= ((NUMERIC '0.05')/ (count(metric_variant_name) over (partition by launch_id)))
                and pct_change_conversion_rate > 0 THEN 1
          ELSE 0
        END AS positive_significant_cr_change,
  -- mean visits
        CASE
        WHEN pval_mean_visits <= ((NUMERIC '0.05')/ (count(metric_variant_name) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_cr_change,
            CASE
        WHEN pval_mean_visits <= ((NUMERIC '0.05')/ (count(metric_variant_name) over (partition by launch_id)))
              and pct_change_mean_visits > 0 THEN 1
        ELSE 0
      END AS positive_significant_mean_visits_change,
  --gms_per_unit

      CASE
        WHEN pval_gms_per_unit <= ((NUMERIC '0.05')/ (count(metric_variant_name) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_gms_per_unit_change,
            CASE
        WHEN pval_gms_per_unit <= ((NUMERIC '0.05')/ (count(metric_variant_name) over (partition by launch_id)))
              and pct_change_gms_per_unit > 0 THEN 1
        ELSE 0
      END AS positive_significant_gms_per_unit_change,


-----------------------------------------------------------------------------
--EXTRAS TO ADD IN IF I CAN 
-----------------------------------------------------------------------------
--includes active
with platforms as (
-- This CTE gets the platform for each experiment (launch_id)
select 
  distinct launch_id
  , update_date
  , name as platform
  , dense_rank() over(partition by launch_id  order by update_date desc) AS row_num
FROM `etsy-data-warehouse-prod.etsy_atlas.catapult_launches_expected_platforms`
qualify row_num = 1
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
, exp_coverage_agg as (
select
  launch_id
  , start_date
  , end_date
  , max(case when coverage_name = 'GMS coverage' then coverage_value else null end) as gms_coverage
  , max(case when coverage_name = 'Traffic coverage' then coverage_value else null end) as traffic_coverage
  , max(case when coverage_name = 'Offsite Ads coverage' then coverage_value else null end) as osa_coverage
  , max(case when coverage_name = 'Prolist coverage' then coverage_value else null end) as prolist_coverage
from exp_coverage
group by  
  all
)
, exp_metrics as (
-- This CTE gathers all the metric ids and the corresponding names included in the experiment, along with whether or not a metric is the success metric.
select
  cem.launch_id
  , cem.metric_id
  , cm.name
  , cem.is_success_criteria
from `etsy-data-warehouse-prod.etsy_atlas.catapult_experiment_metrics`  cem
left join `etsy-data-warehouse-prod.etsy_atlas.catapult_metrics` cm
  on cem.metric_id = cm.metric_id
group by all -- has duplicate rows
)
