-- update the derived table for the active experiment summary explore to include user bucketed experiments 
WITH all_experiments as (
  select
    launch_id
    , date(timestamp_seconds(boundary_start_sec)) as start_date
    , date(timestamp_seconds(boundary_end_sec)) as end_date
    , max(date(timestamp_seconds(boundary_end_sec))) as last_run_date
  from etsy-data-warehouse-prod.catapult.results_metric_day
group by all
having max(date(timestamp_seconds(boundary_end_sec))) >= current_date-1 -- active experiments  only
)
, all_variants AS (
  SELECT
      ae.launch_id,
      ae.start_date,
      case when exp_on_key_metrics.experiment_id is null then "User Bucketed" else "Browser Bucketed" end as bucketing_type, 
      exp_on_key_metrics.experiment_id,
      exp_on_key_metrics.variant_name,
      ae.last_run_date,
      -- DATE(timestamp_seconds(exp_on_key_metrics.bound_start_date)) AS start_date,
      -- DATE(timestamp_seconds(exp_on_key_metrics.run_date)) AS last_run_date,
      exp_on_key_metrics.conv_rate_pct_change,
      exp_on_key_metrics.conv_rate_p_value,
      exp_on_key_metrics.conv_rate_is_powered,
      CASE
        WHEN exp_on_key_metrics.conv_rate_p_value <= NUMERIC '0.10' THEN 1
        ELSE 0
      END AS significant_cr_change,
      exp_on_key_metrics.winsorized_acbv_pct_change,
      exp_on_key_metrics.winsorized_acbv_p_value,
      exp_on_key_metrics.on_gms,
      CASE
        WHEN exp_on_key_metrics.winsorized_acbv_p_value <= NUMERIC '0.10' THEN 1
        ELSE 0
      END AS significant_winsorized_acbv_change,
      exp_on_key_metrics.on_prolist_pct_change,
      exp_on_key_metrics.on_prolist_pct_p_value,
      exp_on_key_metrics.on_prolist_spend * exp_on_key_metrics.on_browsers / 100 AS on_prolist_spend,
      CASE
        WHEN exp_on_key_metrics.on_prolist_pct_p_value <= NUMERIC '0.10' THEN 1
        ELSE 0
      END AS significant_prolist_change,
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
), exp_summary AS (
  SELECT
      a.experiment_id,
      a.launch_id,
      s.team,
      s.name,
      s.initiative,
      a.bucketing_type,
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
      sum(a.on_prolist_spend) AS on_prolist_spend,
      max(a.conv_rate_is_powered) as cr_is_powered,
      max(a.significant_cr_change) AS significant_cr_change,
      max(a.significant_winsorized_acbv_change) AS significant_winsorized_acbv_change,
      max(a.significant_prolist_change) AS significant_prolist_change,
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
        WHEN a.treatment_rank = 3 THEN a.conv_rate_pct_change
      END) AS cr_change_3,
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
      END) AS prolist_p_value_3
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
    a_0.on_gms,
    a_0.on_prolist_spend,
    a_0.cr_is_powered,
    a_0.significant_cr_change,
    a_0.significant_winsorized_acbv_change,
    a_0.significant_prolist_change,
    a_0.cr_change_1,
    a_0.cr_p_value_1,
    a_0.cr_change_2,
    a_0.cr_change_3,
    a_0.winsorized_acbv_change_1,
    a_0.winsorized_acbv_p_value_1,
    a_0.winsorized_acbv_change_2,
    a_0.winsorized_acbv_change_3,
    a_0.prolist_change_1,
    a_0.prolist_change_2,
    a_0.prolist_change_3,
    a_0.prolist_p_value_1,
    a_0.prolist_p_value_2,
    a_0.prolist_p_value_3,
    sum(d.total_gms) AS total_etsy_gms,
    (a_0.off_gms + a_0.on_gms) / sum(d.total_gms) AS gms_coverage,
    (a_0.off_prolist_spend + a_0.on_prolist_spend) / sum(d.total_prolist_spend) AS prolist_coverage
  FROM
    exp_summary AS a_0
    INNER JOIN daily_denoms AS d ON d.date BETWEEN a_0.start_date AND a_0.last_run_date
    left join enabling_teams e on e.launch_id = a_0.experiment_id
   GROUP BY all
ORDER BY
  days_running
