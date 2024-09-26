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
