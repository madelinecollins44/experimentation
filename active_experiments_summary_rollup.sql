-----TAKE BASE FROM EXISTING ROLLUP, NEED TO ADD IN METRICS USING RESULTS_METRICS_DAY
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


  
-------METRICS HERE
WITH  metrics_list as (
-- This CTE grabs all of the metric values from the experiment.
-- Since the results_metric_day table contains values for each day of the experiment (and multiple boundaries if relevant), the date_rnk is used to get the last date of the experiment.
-- There can be multiple values for a metric on the final day (usually if there is a metric that also has a "cuped" value), the metric_rnk is used to grab the metric that has been "cuped" if there
-- are multiple values by choosing the one with the longest metric_stat_methodology. 
select  
  launch_id
  , _date
  , boundary_start_sec as start_date
  , boundary_end_sec as end_date
  , metric_variant_name
  , metric_display_name
  , metric_id
  , metric_value_control
  , metric_value_treatment
  , relative_change
  , p_value
from `etsy-data-warehouse-prod.catapult.results_metric_day` 
where 
  1=1
qualify row_number() over(partition by launch_id, metric_variant_name, metric_display_name order by boundary_start_sec desc) = 1 -- grabs most recent date for each experiment, variant, metric
)
, metrics_agg as (
-- This CTE aggregates all of the relevant metrics. Here is where we can add in new metrics if needed. TO ADD purchase frequency 
select
  ml.launch_id
  , ml.start_date
  , ml.end_date
  ----VARIANT 1 CALCS 
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_mean_visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_mean_visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_mean_visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_mean_visits
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads winsorized acvv ($100)' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_ads_acxv
  , max(case when lower(ml.metric_display_name) = 'ads winsorized acvv ($100)' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_ads_acxv
  , max(case when lower(ml.metric_display_name) = 'ads winsorized acvv ($100)' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_ads_acxv
  , max(case when lower(ml.metric_display_name) = 'ads winsorized acvv ($100)' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_ads_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_ocb
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_ocb
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_ocb
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_ocb
  , max(case when lower(ml.metric_display_name) = 'total orders per unit' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_opu
  , max(case when lower(ml.metric_display_name) = 'total orders per unit' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_opu
  , max(case when lower(ml.metric_display_name) = 'total orders per unit' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_opu
  , max(case when lower(ml.metric_display_name) = 'total orders per unit' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_opu
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_aov
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_aov
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_aov
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_aov  
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_mean_osa_revenue
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_mean_osa_revenue
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_mean_osa_revenue
  ----VARIANT 2 CALCS 
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_mean_osa_revenue
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_mean_visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_mean_visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_mean_visits
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads Conversion Rate' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads Conversion Rate' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads winsorized acvv ($100)' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_ads_acxv
  , max(case when lower(ml.metric_display_name) = 'ads winsorized acvv ($100)' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_ads_acxv
  , max(case when lower(ml.metric_display_name) = 'ads winsorized acvv ($100)' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_ads_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_ocb
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_ocb
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_ocb  
  , max(case when lower(ml.metric_display_name) = 'total orders per unit' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_opu
  , max(case when lower(ml.metric_display_name) = 'total orders per unit' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_opu
  , max(case when lower(ml.metric_display_name) = 'total orders per unit' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_opu  
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_aov
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_aov
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_aov
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_mean_osa_revenue
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_mean_osa_revenue
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_mean_osa_revenue
from metrics_list ml 
left join exp_metrics em
  on em.launch_id = ml.launch_id 
     and em.metric_id = ml.metric_id
group by 
  all
)
/*
-- orders per unit, mean engaged visits, and mean visits coverage calculations
*/
, bucketed_unit_counts as (
select
  cgms.launch_id
  , cgms.start_date
  , cgms.end_date
  , max(case when r.diagnostic_variant_name = "off" then r.diagnostic_variant_value else 0 end) as units_off
  , max(case when r.diagnostic_variant_name <> "off" then r.diagnostic_variant_value else 0 end) as units_on
from `etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports`  cgms 
left join `etsy-data-warehouse-prod.catapult.results_diagnostic_day` r
  on cgms.launch_id = r.launch_id 
  and cgms.end_date = r._date
where 
  r._date >= begin_date
  and r.diagnostic_display_name in ("Total ID count")
group by 
  all
)
, global_visits as (
select
  cgms.launch_id
  , cgms.start_date
  , cgms.end_date
  , count(distinct v.visit_id) as global_visits
  , count(distinct case when engaged_visit_5mins>0 then v.visit_id else null end) as global_engaged_visits
from `etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports`  cgms 
left join `etsy-data-warehouse-prod.visit_mart.visits` v
  on v._date between cgms.start_date and cgms.end_date
where 
  date(timestamp_seconds(v.run_date)) >= begin_date
group by 
  all
)
,global_orders as (
select
  cgms.launch_id
  , cgms.start_date
  , cgms.end_date
  , count(distinct receipt_id) as global_orders
from `etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports`  cgms 
left join `etsy-data-warehouse-prod.transaction_mart.receipts_gms` r
  on date(r.creation_tsz) between cgms.start_date and cgms.end_date
where 
  date(r.creation_tsz) >= begin_date
group by 
  all
)
, experiment_coverage_agg as (
select
  cgms.launch_id
  , cgms.start_date
  , cgms.end_date
  , buc.units_off
  , buc.units_on
  , gv.global_visits
  , gv.global_engaged_visits
  , go.global_orders
from `etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` cgms
left join bucketed_unit_counts buc
  on cgms.launch_id = buc.launch_id
  and cgms.start_date = buc.start_date
  and cgms.end_date = buc.end_date
left join global_visits gv
  on cgms.launch_id = gv.launch_id
  and cgms.start_date = gv.start_date
  and cgms.end_date = gv.end_date
left join global_orders go
  on cgms.launch_id = go.launch_id
  and cgms.start_date = go.start_date
  and cgms.end_date = go.end_date
where
  cgms.start_date >= begin_date
  ) 
 , visit_coverage_calc as (
select
  ca.launch_id
  , ca.start_date
  , ca.end_date
  ,((ca.units_off + ca.units_on) * ma.control_opu) / ca.global_orders as orders_per_unit_coverage
  ,((ca.units_off + ca.units_on) * ma.control_mean_engaged_visit) / ca.global_engaged_visits as engaged_visit_coverage
  ,((ca.units_off + ca.units_on) * ma.control_mean_visits) / ca.global_visits as mean_visit_coverage
from experiment_coverage_agg ca
left join metrics_agg ma
  on ca.launch_id = ma.launch_id
  and ca.start_date = date(timestamp_seconds(ma.start_date))
  and ca.end_date = date(timestamp_seconds(ma.end_date))
  )  
  
select
  cgms.launch_id
  , concat("https://atlas.etsycorp.com/catapult/", CAST(cgms.launch_id AS STRING)) AS catapult_link
  , cgms.gms_report_id
  , cgms.is_long_term_holdout
  , eb.bucket_type
  , cgms.experiment_name
  , cl.hypothesis
  , cgms.start_date
  , cgms.end_date
  , cgms.learnings
  , coalesce(cgms.initiative, cl.initiative) as initiative
  , cgms.subteam 
  , cgms.product_lead
  , cgms.analyst_lead
  , cgms.status
  , coalesce(p.platform,cgms.platform) as platform
  , et.enabling_teams
  , coalesce(ep.unavailable_listing,cgms.unavailable_listing) as unavailable_listing_page
  , coalesce(ep.category,cgms.category) as category_page
  , coalesce(ep.checkout,cgms.checkout) as checkout_page
  , coalesce(ep.cart,cgms.cart) as cart_page
  , coalesce(ep.other,cgms.other) as other_page
  , coalesce(ep.listing,cgms.listing) as listing_page
  , coalesce(ep.market,cgms.market) as market_page
  , coalesce(ep.sitewide,cgms.sitewide) as sitewide_page
  , coalesce(ep.shop_home,cgms.shop_home) as shop_home_page
  , coalesce(ep.home,cgms.home) as home_page
  , coalesce(ep.sold_out_listing,cgms.sold_out_listing) as sold_out_listing_page
  , coalesce(ep.search,cgms.search) as search_page
  , coalesce(cgms.audience,cl.audience) AS audience
  , coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) AS traffic_percentage
  , eca.gms_coverage
  , eca.traffic_coverage
  , eca.osa_coverage
  , eca.prolist_coverage
  , cgms.kpi_initiative_name
  , cgms.kpi_initiative_value
  , cgms.kpi_initiative_coverage
  , cgms.kr_metric_name
  , cgms.kr_metric_value
  , cgms.kr_metric_coverage
  , cgms.kr_metric_name_2 as kr2_metric_name
  , cgms.kr_metric_value_2 as kr2_metric_value
  , cgms.kr_metric_coverage_2 as kr2_metric_coverage
  , ma.target_metric
  , ma.control_value_target_metric
  , ma.variant1_value_target_metric
  , ma.variant1_pct_change_target_metric
  , ma.variant1_pval_target_metric
  , ma.control_conversion_rate
  , ma.variant1_conversion_rate
  , ma.variant1_pct_change_conversion_rate
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_conversion_rate * eca.gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_conversion_rate
  , ma.variant1_pval_conversion_rate
  , ma.control_mean_visits
  , ma.variant1_mean_visits
  , ma.variant1_pct_change_mean_visits
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_mean_visits * vcc.mean_visit_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_mean_visits
  , ma.variant1_pval_mean_visits
  , ma.control_gms_per_unit
  , ma.variant1_gms_per_unit
  , ma.variant1_pct_change_gms_per_unit
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_gms_per_unit * eca.gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_gms_per_unit
  , ma.variant1_pval_gms_per_unit
  , ma.control_mean_engaged_visit
  , ma.variant1_mean_engaged_visit
  , ma.variant1_pct_change_mean_engaged_visit
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_mean_engaged_visit * vcc.engaged_visit_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_mean_engaged_visit
  , ma.variant1_pval_mean_engaged_visit
  , ma.control_winsorized_acxv
  , ma.variant1_winsorized_acxv
  , ma.variant1_pct_change_winsorized_acxv
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_winsorized_acxv * eca.gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_acxv
  , ma.variant1_pval_winsorized_acxv
  , ma.control_ocb
  , ma.variant1_ocb
  , ma.variant1_pct_change_ocb
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_ocb * eca.gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_ocb
  , ma.variant1_pval_ocb
  , ma.control_opu
  , ma.variant1_opu
  , ma.variant1_pct_change_opu
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_opu * vcc.orders_per_unit_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_mean_opu
  , ma.variant1_pval_opu
  , ma.control_aov
  , ma.variant1_aov
  , ma.variant1_pct_change_aov
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_aov * eca.gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_mean_aov
  , ma.variant1_pval_aov
  , ma.control_ads_cvr
  , ma.variant1_ads_cvr
  , ma.variant1_pct_change_ads_cvr
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_ads_cvr * adc.ad_gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_ads_cvr
  , ma.variant1_pval_ads_cvr
  , ma.control_ads_acxv
  , ma.variant1_ads_acxv
  , ma.variant1_pct_change_ads_acxv
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_ads_acxv * adc.ad_gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_ads_acxv
  , ma.variant1_pval_ads_acxv
  , ma.control_mean_prolist_spend
  , ma.variant1_mean_prolist_spend
  , ma.variant1_pct_change_mean_prolist_spend
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_mean_prolist_spend * eca.prolist_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_prolist_spend
  , ma.variant1_pval_mean_prolist_spend
  , ma.control_mean_osa_revenue
  , ma.variant1_mean_osa_revenue
  , ma.variant1_pct_change_mean_osa_revenue
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant1_pct_change_mean_osa_revenue * eca.osa_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_osa_revenue
  , ma.variant1_pval_mean_osa_revenue
  , ma.variant2_conversion_rate
  , ma.variant2_pct_change_conversion_rate
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_conversion_rate * eca.gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_conversion_rate
  , ma.variant2_pval_conversion_rate
  , ma.variant2_mean_visits
  , ma.variant2_pct_change_mean_visits
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_mean_visits * vcc.mean_visit_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_mean_visits
  , ma.variant2_pval_mean_visits
  , ma.variant2_gms_per_unit
  , ma.variant2_pct_change_gms_per_unit
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_gms_per_unit * eca.gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_gms_per_unit
  , ma.variant2_pval_gms_per_unit
  , ma.variant2_mean_engaged_visit
  , ma.variant2_pct_change_mean_engaged_visit
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_mean_engaged_visit * vcc.engaged_visit_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_mean_engaged_visit
  , ma.variant2_pval_mean_engaged_visit
  , ma.variant2_winsorized_acxv
  , ma.variant2_pct_change_winsorized_acxv
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_winsorized_acxv * adc.ad_gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_ads_acxv
  , ma.variant2_pval_winsorized_acxv
  , ma.variant2_ocb
  , ma.variant2_pct_change_ocb
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_ocb * eca.gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_ocb
  , ma.variant2_pval_ocb  
  , ma.variant2_opu
  , ma.variant2_pct_change_opu
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_opu * vcc.orders_per_unit_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_mean_opu
  , ma.variant2_pval_opu  
  , ma.variant2_aov
  , ma.variant2_pct_change_aov
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_aov * eca.gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_mean_aov
  , ma.variant2_pval_aov
  , ma.variant2_ads_acxv
  , ma.variant2_pct_change_ads_acxv
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_ads_acxv * eca.gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_acxv
  , ma.variant2_pval_ads_acxv
  , ma.variant2_ads_cvr
  , ma.variant2_pct_change_ads_cvr
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_ads_cvr * adc.ad_gms_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_ads_cvr
  , ma.variant2_pval_ads_cvr
  , ma.variant2_mean_prolist_spend
  , ma.variant2_pct_change_mean_prolist_spend
  -- Global Calculation: metric precentage lift * metric coverage * traffic percentage
  , ma.variant2_pct_change_mean_prolist_spend * eca.prolist_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_prolist_spend
  , ma.variant2_pval_mean_prolist_spend
  , ma.variant2_mean_osa_revenue
  , ma.variant2_pct_change_mean_osa_revenue
  , ma.variant2_pct_change_mean_osa_revenue * eca.osa_coverage * coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) as global_variant2_osa_revenue
  , ma.variant2_pval_mean_osa_revenue
from `etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports`  cgms
left join `etsy-data-warehouse-prod.etsy_atlas.catapult_launches` cl
  on cgms.launch_id = cl.launch_id
left join plats_agg p 
  on cgms.launch_id = p.launch_id
left join experiment_enabling_teams et
  on cgms.launch_id = et.launch_id
left join experiment_pages_ran_on ep
  on cgms.launch_id = ep.launch_id
left join experiment_bucket_type eb
  on cgms.launch_id = eb.launch_id
left join exp_coverage_agg eca
  on cgms.launch_id = eca.launch_id
  and cgms.start_date = eca.start_date
  and cgms.end_date = eca.end_date
left join `etsy-data-warehouse-prod.rollups.experiment_ads_coverage` adc
  on cgms.launch_id = adc.launch_id
  and cgms.start_date = adc.start_date
  and cgms.end_date = adc.end_date
left join metrics_agg ma 
  on cgms.launch_id = ma.launch_id
  and cgms.start_date = date(timestamp_seconds(ma.start_date))
  and cgms.end_date = date(timestamp_seconds(ma.end_date))
left join visit_coverage_calc vcc
  on cgms.launch_id = vcc.launch_id
  and cgms.start_date = vcc.start_date
  and cgms.end_date = vcc.end_date
where
  extract(year from cgms.start_date)>=2024
  and cgms.launch_id is not null
);


-----EXTRAS TO ADD IN IF I CAN 
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
