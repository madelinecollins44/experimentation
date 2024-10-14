-- update the derived table for the active experiment summary explore to include user bucketed experiments 
--------Entire Query
WITH all_experiments as (
  SELECT
  DISTINCT l.launch_id,
  date(boundary_start_ts) AS start_date,
  _date AS last_run_date
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
, all_variants_minus_stat_sig AS (
  SELECT
      ae.launch_id,
      ae.start_date,
      ae.last_run_date,
      case when exp_on_key_metrics.experiment_id is null then "User Bucketed" else "Browser Bucketed" end as bucketing_type,
      exp_on_key_metrics.experiment_id,
      exp_on_key_metrics.variant_name,
      -- DATE(timestamp_seconds(exp_on_key_metrics.bound_start_date)) AS start_date,
      -- DATE(timestamp_seconds(exp_on_key_metrics.run_date)) AS last_run_date,
    exp_on_key_metrics.on_browsers,
 --conversion rate metrics
      exp_on_key_metrics.conv_rate_pct_change,
      exp_on_key_metrics.conv_rate_p_value,
      exp_on_key_metrics.conv_rate_is_powered,
--acbv metrics
      exp_on_key_metrics.winsorized_acbv_pct_change,
      exp_on_key_metrics.winsorized_acbv_p_value,
      exp_on_key_metrics.on_gms,
--prolist metrics
      exp_on_key_metrics.on_prolist_pct_change,
      exp_on_key_metrics.on_prolist_pct_p_value,
      exp_on_key_metrics.on_prolist_spend * exp_on_key_metrics.on_browsers / 100 AS on_prolist_spend,
--add to cart metrics
      exp_on_key_metrics.on_atc_pct_change,
      exp_on_key_metrics.on_atc_p_value,
--checkout start metrics
      exp_on_key_metrics.on_checkout_start_pct_change,
      exp_on_key_metrics.on_checkout_start_p_value,
--error page vw metrics
      exp_on_key_metrics.on_error_pg_vw_pct_change,
      exp_on_key_metrics.on_error_pg_vw_p_value,
--visit frequency metrics
      exp_on_key_metrics.on_visit_freq_pct_change,
      exp_on_key_metrics.on_visit_freq_p_value,
      exp_on_key_metrics.on_visit_freq_pct_change_cuped,
-- osa revenue metrics
      exp_on_key_metrics.on_osa_revenue_attribution_pct_change,
      exp_on_key_metrics.on_osa_revenue_attribution_p_value,
--ocb page metrics
      exp_on_key_metrics.on_ocb_pct_change,
      exp_on_key_metrics.on_ocb_p_value,
--aov metrics
      exp_on_key_metrics.on_winsorized_aov_pct_change,
      exp_on_key_metrics.on_winsorized_aov_p_value,
--engaged metrics
      exp_on_key_metrics.on_engaged_visits,
      exp_on_key_metrics.on_engaged_visits_pct_change,
      exp_on_key_metrics.on_engaged_visits_p_value,
-- bounce metrics
      exp_on_key_metrics.on_bounces_pct_change,
      exp_on_key_metrics.on_bounces_p_value,
--total orders per browser orders metrics
      exp_on_key_metrics.on_total_orders_per_browser,
      exp_on_key_metrics.on_total_orders_per_browser_pct_change,
      exp_on_key_metrics.on_total_orders_per_browser_p_value,
--purchase frequency metrics
      exp_on_key_metrics.on_purchase_freq_pct_change,
      exp_on_key_metrics.on_purchase_freq_p_value,
      exp_on_key_metrics.on_purchase_freq_pct_change_cuped,
      row_number() OVER (PARTITION BY exp_on_key_metrics.experiment_id ORDER BY exp_on_key_metrics.conv_rate_p_value) AS treatment_rank
  FROM
    all_experiments ae
  left join
    `etsy-data-warehouse-prod`.catapult.exp_on_key_metrics
      on ae.launch_id=exp_on_key_metrics.experiment_id
      and exp_on_key_metrics.segmentation = 'any'
    where (exp_on_key_metrics.experiment_id is null or exp_on_key_metrics.run_date IN (
      SELECT
          max(exp_on_key_metrics_0.run_date)
        FROM
          `etsy-data-warehouse-prod`.catapult.exp_on_key_metrics AS exp_on_key_metrics_0)
    )
  and ae.launch_id NOT IN(
      SELECT
          launch_id
        FROM
          `etsy-data-warehouse-prod`.etsy_atlas.catapult_launches
        WHERE launch_percentage IN (0, 100)
    )
  ORDER BY 1,2
    --  exclude ramped experiments
)
, all_variants as (
select *,
 count(treatment_rank) over (partition by launch_id) as treatments_per_experiment,
 ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) as stat_sign_thresold,
 --conversion rate metrics
      CASE
        WHEN conv_rate_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_cr_change,
            CASE
        WHEN conv_rate_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
              and conv_rate_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_cr_change,
--acbv metrics
      CASE
        WHEN winsorized_acbv_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_winsorized_acbv_change,
    CASE
        WHEN winsorized_acbv_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
              and winsorized_acbv_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_winsorized_acbv_change,
--prolist metrics
      CASE
        WHEN on_prolist_pct_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_prolist_change,
     CASE
        WHEN on_prolist_pct_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
              and on_prolist_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_prolist_change,
--add to cart metrics
      CASE
        WHEN on_atc_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_atc_change,
      CASE
        WHEN on_atc_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_atc_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_atc_change,
--checkout start metrics
      CASE
        WHEN on_checkout_start_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_checkout_start_change,
       CASE
        WHEN on_checkout_start_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_checkout_start_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_checkout_start_change,
--error page vw metrics
      CASE
        WHEN on_error_pg_vw_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_error_pg_vw_change,
     CASE
        WHEN on_error_pg_vw_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_error_pg_vw_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_error_pg_vw_change,
--visit frequency metrics
      CASE
        WHEN on_visit_freq_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_visit_freq_change,
          CASE
        WHEN on_visit_freq_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_visit_freq_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_visit_freq_change,
-- osa revenue metrics
      CASE
        WHEN on_osa_revenue_attribution_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_osa_revenue_attribution_change,
       CASE
        WHEN on_osa_revenue_attribution_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_osa_revenue_attribution_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_osa_revenue_attribution_change,
--ocb page metrics
      CASE
        WHEN on_ocb_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_ocb_change,
       CASE
        WHEN on_ocb_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_ocb_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_ocb_change,
--aov metrics
      CASE
        WHEN on_winsorized_aov_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_winsorized_aov_change,
      CASE
        WHEN on_winsorized_aov_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_winsorized_aov_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_winsorized_aov_change,
--engaged metrics
      CASE
        WHEN on_engaged_visits_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_engaged_visits_change,
    CASE
        WHEN on_engaged_visits_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_engaged_visits_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_engaged_visits_change,
-- bounce metrics
      CASE
        WHEN on_bounces_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_bounces_change,
     CASE
        WHEN on_bounces_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_bounces_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_bounces_change,
--total orders per browser orders metrics
      CASE
        WHEN on_total_orders_per_browser_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_total_orders_per_browser_change,
    CASE
        WHEN on_total_orders_per_browser_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_total_orders_per_browser_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_total_orders_per_browser_change,
--purchase frequency metrics
      CASE
        WHEN on_purchase_freq_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_purchase_freq_change,
     CASE
        WHEN on_purchase_freq_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) 
        and on_purchase_freq_pct_change > 0 THEN 1
        ELSE 0
      END AS positive_significant_purchase_freq_change,
from all_variants_minus_stat_sig
group by all)
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
      a.experiment_id,
      a.launch_id,
      s.team,
      s.name,
      s.initiative,
      a.bucketing_type,
      a.treatments_per_experiment,
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
      sum(a.on_gms) AS on_gms,
      sum(a.on_gms) / count(distinct a.on_browsers) AS gms_per_unit,
      sum(a.on_prolist_spend) AS on_prolist_spend,
      sum(a.on_engaged_visits) as on_engaged_visits,
      max(a.conv_rate_is_powered) as cr_is_powered,
      -- sig changes
      max(a.significant_cr_change) AS significant_cr_change,
      max(a.significant_winsorized_acbv_change) AS significant_winsorized_acbv_change,
      max(a.significant_prolist_change) AS significant_prolist_change,
      max(a.significant_atc_change) AS significant_atc_change,
      max(a.significant_checkout_start_change) AS significant_checkout_start_change,
      max(a.significant_error_pg_vw_change) AS significant_error_pg_vw_change,
      max(a.significant_visit_freq_change) AS significant_visit_freq_change,
      max(a.significant_osa_revenue_attribution_change) AS significant_osa_revenue_attribution_change,
      max(a.significant_ocb_change) AS significant_ocb_change,
      max(a.significant_winsorized_aov_change) AS significant_winsorized_aov_change,
      max(a.significant_engaged_visits_change) AS significant_engaged_visits_change,
      max(a.significant_bounces_change) AS significant_bounces_change,
      max(a.significant_total_orders_per_browser_change) AS significant_total_orders_per_browser_change,
      max(a.significant_purchase_freq_change) AS significant_purchase_freq_change,
      -- positive changes
      max(a.positive_significant_cr_change) AS positive_significant_cr_change,
      max(a.positive_significant_winsorized_acbv_change) AS positive_significant_winsorized_acbv_change,
      max(a.positive_significant_prolist_change) AS positive_significant_prolist_change,
      max(a.positive_significant_atc_change) AS positive_significant_atc_change,
      max(a.positive_significant_checkout_start_change) AS positive_significant_checkout_start_change,
      max(a.positive_significant_error_pg_vw_change) AS positive_significant_error_pg_vw_change,
      max(a.positive_significant_visit_freq_change) AS positive_significant_visit_freq_change,
      max(a.positive_significant_osa_revenue_attribution_change) AS positive_significant_osa_revenue_attribution_change,
      max(a.positive_significant_ocb_change) AS positive_significant_ocb_change,
      max(a.positive_significant_winsorized_aov_change) AS positive_significant_winsorized_aov_change,
      max(a.positive_significant_engaged_visits_change) AS positive_significant_engaged_visits_change,
      max(a.positive_significant_bounces_change) AS positive_significant_bounces_change,
      max(a.positive_significant_total_orders_per_browser_change) AS positive_significant_total_orders_per_browser_change,
      max(a.positive_significant_purchase_freq_change) AS positive_significant_purchase_freq_change,
     --cr
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.conv_rate_pct_change
      END) AS cr_change_1,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.conv_rate_p_value
      END) AS cr_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.conv_rate_pct_change
      END) AS cr_change_2,
            max(CASE
        WHEN a.treatment_rank = 2 THEN a.conv_rate_p_value
      END) AS cr_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.conv_rate_pct_change
      END) AS cr_change_3,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.conv_rate_p_value
      END) AS cr_p_value_3,
      --acvv
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.winsorized_acbv_pct_change
      END) AS winsorized_acbv_change_1,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.winsorized_acbv_p_value
      END) AS winsorized_acbv_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.winsorized_acbv_pct_change
      END) AS winsorized_acbv_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.winsorized_acbv_pct_change
      END) AS winsorized_acbv_change_3,
     --prolist
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_prolist_pct_change
      END) AS prolist_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_prolist_pct_change
      END) AS prolist_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_prolist_pct_change
      END) AS prolist_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_prolist_pct_p_value
      END) AS prolist_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_prolist_pct_p_value
      END) AS prolist_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_prolist_pct_p_value
      END) AS prolist_p_value_3,
   --atc
         max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_atc_pct_change
      END) AS atc_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_atc_pct_change
      END) AS atc_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_atc_pct_change
      END) AS atc_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_atc_p_value
      END) AS atc_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_atc_p_value
      END) AS atc_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_atc_p_value
      END) AS atc_p_value_3,
--checkout start metrics
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_checkout_start_pct_change
      END) AS checkout_start_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_checkout_start_pct_change
      END) AS checkout_start_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_checkout_start_pct_change
      END) AS checkout_start_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_checkout_start_p_value
      END) AS checkout_start_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_checkout_start_p_value
      END) AS checkout_start_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_checkout_start_p_value
      END) AS checkout_start_p_value_3,
--error page vw metrics
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_error_pg_vw_pct_change
      END) AS error_pg_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_error_pg_vw_pct_change
      END) AS error_pg_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_error_pg_vw_pct_change
      END) AS error_pg_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_error_pg_vw_p_value
      END) AS error_pg_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_error_pg_vw_p_value
      END) AS error_pg_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_error_pg_vw_p_value
      END) AS error_pg_p_value_3,
--visit frequency metrics
    max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_visit_freq_pct_change
      END) AS visit_freq_p_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_visit_freq_pct_change
      END) AS visit_freq_p_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_visit_freq_pct_change
      END) AS visit_freq_p_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_visit_freq_p_value
      END) AS visit_freq_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_visit_freq_p_value
      END) AS visit_freq_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_visit_freq_p_value
      END) AS visit_freq_p_value_3,
-- osa revenue metrics
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_osa_revenue_attribution_pct_change
      END) AS osa_revenue_attribution_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_osa_revenue_attribution_pct_change
      END) AS osa_revenue_attribution_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_osa_revenue_attribution_pct_change
      END) AS osa_revenue_attribution_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_osa_revenue_attribution_p_value
      END) AS osa_revenue_attribution_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_osa_revenue_attribution_p_value
      END) AS osa_revenue_attribution_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_osa_revenue_attribution_p_value
      END) AS osa_revenue_attribution_p_value_3,
--ocb page metrics
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_ocb_pct_change
      END) AS ocb_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_ocb_pct_change
      END) AS ocb_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_ocb_pct_change
      END) AS ocb_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_ocb_p_value
      END) AS ocb_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_ocb_p_value
      END) AS ocb_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_ocb_p_value
      END) AS ocb_p_value_3,
--aov metrics
     max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_winsorized_aov_pct_change
      END) AS winsorized_aov_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_winsorized_aov_pct_change
      END) AS winsorized_aov_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_winsorized_aov_pct_change
      END) AS winsorized_aov_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_winsorized_aov_p_value
      END) AS winsorized_aov_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_winsorized_aov_p_value
      END) AS winsorized_aov_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_winsorized_aov_p_value
      END) AS winsorized_aov_p_value_3,
--engaged metrics
  max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_engaged_visits_pct_change
      END) AS engaged_visits_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_engaged_visits_pct_change
      END) AS engaged_visits_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_engaged_visits_pct_change
      END) AS engaged_visits_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_engaged_visits_p_value
      END) AS engaged_visits_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_engaged_visits_p_value
      END) AS engaged_visits_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_engaged_visits_p_value
      END) AS engaged_visits_p_value_3,
-- bounce metrics
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_bounces_pct_change
      END) AS bounces_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_bounces_pct_change
      END) AS bounces_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_bounces_pct_change
      END) AS bounces_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_bounces_p_value
      END) AS bounces_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_bounces_p_value
      END) AS bounces_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_bounces_p_value
      END) AS bounces_p_value_3,
--total orders per browser orders metrics
       max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_total_orders_per_browser_pct_change
      END) AS total_orders_per_browser_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_total_orders_per_browser_pct_change
      END) AS total_orders_per_browser_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_total_orders_per_browser_pct_change
      END) AS total_orders_per_browser_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_total_orders_per_browser_p_value
      END) AS total_orders_per_browser_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_total_orders_per_browser_p_value
      END) AS total_orders_per_browser_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_total_orders_per_browser_p_value
      END) AS total_orders_per_browser_p_value_3,
--purchase frequency metrics
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_purchase_freq_pct_change
      END) AS purchase_freq_change_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_purchase_freq_pct_change
      END) AS purchase_freq_change_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_purchase_freq_pct_change
      END) AS purchase_freq_change_3,
      max(CASE
        WHEN a.treatment_rank = 1 THEN a.on_purchase_freq_p_value
      END) AS purchase_freq_p_value_1,
      max(CASE
        WHEN a.treatment_rank = 2 THEN a.on_purchase_freq_p_value
      END) AS purchase_freq_p_value_2,
      max(CASE
        WHEN a.treatment_rank = 3 THEN a.on_purchase_freq_p_value
      END) AS purchase_freq_p_value_3
    FROM
      all_variants AS a
      LEFT JOIN off_gms AS o ON a.experiment_id = o.experiment_id
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
        GROUP BY launch_id)
SELECT
    a_0.experiment_id,
    a_0.launch_id,
    a_0.bucketing_type,
    a_0.treatments_per_experiment,
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
    a_0.off_gms,
    a_0.off_prolist_spend,
  on_gms,
  gms_per_unit,
 on_prolist_spend,
  on_engaged_visits,
  cr_is_powered,
  significant_cr_change,
  significant_winsorized_acbv_change,
  significant_prolist_change,
   significant_atc_change,
   significant_checkout_start_change,
   significant_error_pg_vw_change,
   significant_visit_freq_change,
   significant_osa_revenue_attribution_change,
   significant_ocb_change,
   significant_winsorized_aov_change,
   significant_engaged_visits_change,
   significant_bounces_change,
   significant_total_orders_per_browser_change,
   significant_purchase_freq_change,
     positive_significant_cr_change,
  positive_significant_winsorized_acbv_change,
  positive_significant_prolist_change,
positive_significant_atc_change,
   positive_significant_checkout_start_change,
   positive_significant_error_pg_vw_change,
   positive_significant_visit_freq_change,
   positive_significant_osa_revenue_attribution_change,
   positive_significant_ocb_change,
   positive_significant_winsorized_aov_change,
   positive_significant_engaged_visits_change,
   positive_significant_bounces_change,
   positive_significant_total_orders_per_browser_change,
   positive_significant_purchase_freq_change,
   cr_change_1,
   cr_p_value_1,
      cr_p_value_2,
   cr_change_2,
      cr_p_value_3,
    cr_change_3,
    winsorized_acbv_change_1,
    winsorized_acbv_p_value_1,
    winsorized_acbv_change_2,
     winsorized_acbv_change_3,
    prolist_change_1,
    prolist_change_2,
    prolist_change_3,
    prolist_p_value_1,
    prolist_p_value_2,
    prolist_p_value_3,
    atc_change_1,
    atc_change_2,
    atc_change_3,
    atc_p_value_1,
    atc_p_value_2,
    atc_p_value_3,
    checkout_start_change_1,
    checkout_start_change_2,
    checkout_start_change_3,
    checkout_start_p_value_1,
    checkout_start_p_value_2,
    checkout_start_p_value_3,
    error_pg_change_1,
    error_pg_change_2,
    error_pg_change_3,
    error_pg_p_value_1,
    error_pg_p_value_2,
    error_pg_p_value_3,
    visit_freq_p_change_1,
    visit_freq_p_change_2,
    visit_freq_p_change_3,
    visit_freq_p_value_1,
    visit_freq_p_value_2,
    visit_freq_p_value_3,
    osa_revenue_attribution_change_1,
    osa_revenue_attribution_change_2,
    osa_revenue_attribution_change_3,
    osa_revenue_attribution_p_value_1,
    osa_revenue_attribution_p_value_2,
    osa_revenue_attribution_p_value_3,
  ocb_change_1,
  ocb_change_2,
  ocb_change_3,
  ocb_p_value_1,
  ocb_p_value_2,
  ocb_p_value_3,
  winsorized_aov_change_1,
  winsorized_aov_change_2,
  winsorized_aov_change_3,
  winsorized_aov_p_value_1,
  winsorized_aov_p_value_2,
  winsorized_aov_p_value_3,
  engaged_visits_change_1,
  engaged_visits_change_2,
  engaged_visits_change_3,
  engaged_visits_p_value_1,
  engaged_visits_p_value_2,
  engaged_visits_p_value_3,
  bounces_change_1,
  bounces_change_2,
  bounces_change_3,
  bounces_p_value_1,
  bounces_p_value_2,
  bounces_p_value_3,
  total_orders_per_browser_change_1,
  total_orders_per_browser_change_2,
  total_orders_per_browser_change_3,
  total_orders_per_browser_p_value_1,
  total_orders_per_browser_p_value_2,
  total_orders_per_browser_p_value_3,
  purchase_freq_change_1,
  purchase_freq_change_2,
  purchase_freq_change_3,
  purchase_freq_p_value_1,
  purchase_freq_p_value_2,
  purchase_freq_p_value_3,
    sum(d.total_gms) AS total_etsy_gms,
    (a_0.off_gms + a_0.on_gms) / sum(d.total_gms) AS gms_coverage,
    (a_0.off_prolist_spend + a_0.on_prolist_spend) / sum(d.total_prolist_spend) AS prolist_coverage
  FROM
    exp_summary AS a_0
    INNER JOIN daily_denoms AS d ON d.date BETWEEN a_0.start_date AND a_0.last_run_date
    left join enabling_teams e on e.launch_id = a_0.experiment_id
where (experiment_id = 1305831155216 or experiment_id = 1304533669483)
   GROUP BY all
ORDER BY
  days_running









----------------------test
-- stat sig calcs
WITH all_experiments as (
  SELECT
  DISTINCT l.launch_id,
  date(boundary_start_ts) AS start_date,
  _date AS last_run_date
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
, all_variants_minus_stat_sig AS (
  SELECT
      ae.launch_id,
      ae.start_date,
      ae.last_run_date,
      case when exp_on_key_metrics.experiment_id is null then "User Bucketed" else "Browser Bucketed" end as bucketing_type,
      exp_on_key_metrics.experiment_id,
      exp_on_key_metrics.variant_name,
      -- DATE(timestamp_seconds(exp_on_key_metrics.bound_start_date)) AS start_date,
      -- DATE(timestamp_seconds(exp_on_key_metrics.run_date)) AS last_run_date,
    exp_on_key_metrics.on_browsers,
 --conversion rate metrics
      exp_on_key_metrics.conv_rate_pct_change,
      exp_on_key_metrics.conv_rate_p_value,
      exp_on_key_metrics.conv_rate_is_powered,
--acbv metrics
      exp_on_key_metrics.winsorized_acbv_pct_change,
      exp_on_key_metrics.winsorized_acbv_p_value,
      exp_on_key_metrics.on_gms,
--prolist metrics
      exp_on_key_metrics.on_prolist_pct_change,
      exp_on_key_metrics.on_prolist_pct_p_value,
      exp_on_key_metrics.on_prolist_spend * exp_on_key_metrics.on_browsers / 100 AS on_prolist_spend,
--add to cart metrics
      exp_on_key_metrics.on_atc_pct_change,
      exp_on_key_metrics.on_atc_p_value,
--checkout start metrics
      exp_on_key_metrics.on_checkout_start_pct_change,
      exp_on_key_metrics.on_checkout_start_p_value,
--error page vw metrics
      exp_on_key_metrics.on_error_pg_vw_pct_change,
      exp_on_key_metrics.on_error_pg_vw_p_value,
--visit frequency metrics
      exp_on_key_metrics.on_visit_freq_pct_change,
      exp_on_key_metrics.on_visit_freq_p_value,
      exp_on_key_metrics.on_visit_freq_pct_change_cuped,
-- osa revenue metrics
      exp_on_key_metrics.on_osa_revenue_attribution_pct_change,
      exp_on_key_metrics.on_osa_revenue_attribution_p_value,
--ocb page metrics
      exp_on_key_metrics.on_ocb_pct_change,
      exp_on_key_metrics.on_ocb_p_value,
--aov metrics
      exp_on_key_metrics.on_winsorized_aov_pct_change,
      exp_on_key_metrics.on_winsorized_aov_p_value,
--engaged metrics
      exp_on_key_metrics.on_engaged_visits,
      exp_on_key_metrics.on_engaged_visits_pct_change,
      exp_on_key_metrics.on_engaged_visits_p_value,
-- bounce metrics
      exp_on_key_metrics.on_bounces_pct_change,
      exp_on_key_metrics.on_bounces_p_value,
--total orders per browser orders metrics
      exp_on_key_metrics.on_total_orders_per_browser,
      exp_on_key_metrics.on_total_orders_per_browser_pct_change,
      exp_on_key_metrics.on_total_orders_per_browser_p_value,
--purchase frequency metrics
      exp_on_key_metrics.on_purchase_freq_pct_change,
      exp_on_key_metrics.on_purchase_freq_p_value,
      exp_on_key_metrics.on_purchase_freq_pct_change_cuped,
      row_number() OVER (PARTITION BY exp_on_key_metrics.experiment_id ORDER BY exp_on_key_metrics.conv_rate_p_value) AS treatment_rank
  FROM
    all_experiments ae
  left join
    `etsy-data-warehouse-prod`.catapult.exp_on_key_metrics
      on ae.launch_id=exp_on_key_metrics.experiment_id
      and exp_on_key_metrics.segmentation = 'any'
    where (exp_on_key_metrics.experiment_id is null or exp_on_key_metrics.run_date IN (
      SELECT
          max(exp_on_key_metrics_0.run_date)
        FROM
          `etsy-data-warehouse-prod`.catapult.exp_on_key_metrics AS exp_on_key_metrics_0)
    )
  and ae.launch_id NOT IN(
      SELECT
          launch_id
        FROM
          `etsy-data-warehouse-prod`.etsy_atlas.catapult_launches
        WHERE launch_percentage IN (0, 100)
    )
  ORDER BY 1,2
    --  exclude ramped experiments
)
-- , all_variants as
select *,
 count(treatment_rank) over (partition by launch_id) as treatments_per_experiment,
 ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) as stat_sign_thresold,
 --conversion rate metrics
      CASE
        WHEN conv_rate_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_cr_change,
--acbv metrics
      CASE
        WHEN winsorized_acbv_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_winsorized_acbv_change,
--prolist metrics
      CASE
        WHEN on_prolist_pct_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_prolist_change,
--add to cart metrics
      CASE
        WHEN on_atc_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_atc_change,
--checkout start metrics
      CASE
        WHEN on_checkout_start_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_checkout_start_change,
--error page vw metrics
      CASE
        WHEN on_error_pg_vw_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_error_pg_vw_change,
--visit frequency metrics
      CASE
        WHEN on_visit_freq_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_visit_freq_change,
-- osa revenue metrics
      CASE
        WHEN on_osa_revenue_attribution_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_osa_revenue_attribution_change,
--ocb page metrics
      CASE
        WHEN on_ocb_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_ocb_change,
--aov metrics
      CASE
        WHEN on_winsorized_aov_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_winsorized_aov_change,
--engaged metrics
      CASE
        WHEN on_engaged_visits_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_engaged_visits_change,
-- bounce metrics
      CASE
        WHEN on_bounces_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_bounces_change,
--total orders per browser orders metrics
      CASE
        WHEN on_total_orders_per_browser_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id)))  THEN 1
        ELSE 0
      END AS significant_total_orders_per_browser_change,
--purchase frequency metrics
      CASE
        WHEN on_purchase_freq_p_value <= ((NUMERIC '0.05')/ (count(treatment_rank) over (partition by launch_id))) THEN 1
        ELSE 0
      END AS significant_purchase_freq_change
from all_variants_minus_stat_sig
where launch_id = 1305831155216
group by all 

------check to make sure all active expeirments are accounted for
  ------on 9/26, explore showed: browser: 65, user: 18
--check source of truth
SELECT bucketing_id_type, count(distinct experiment_id) as number_experiments
FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
WHERE _date = "2024-09-25"
  AND (bucketing_id_type = 1 or bucketing_id_type = 2)
    group by all
--1: 67
--2: 19

--check # of experiments in explore
  SELECT
  count(distinct l.launch_id) as experiments,
  bucketing_id_type
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
group by all 
-- 1: 69
-- 2: 19
