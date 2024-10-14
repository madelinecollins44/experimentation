with raw_data as (
select
  _date, 
  experiment_id,
  lower(name) as metric_name,
  -- case  
  --    when segmentation in ('is_tablet') and segment in ('1') then 'tablet'
  --    when segmentation in ('is_tablet') and segment in ('0') then 'not tablet'
  --    else 'control'
  -- end as is_tablet,
  population_1_name as control,
  population_2_name as treatment,
  p_value, 
  significance,
  power as _power, 
  percent_change
from `etsy-data-warehouse-prod.catapult.catapult_metrics_results` 
inner join etsy-data-warehouse-prod.etsy_atlas.catapult_metrics using (metric_id)
where 
  lower(name) in ('conversion rate','winsorized ac*v','orders per unit','winsorized aov','mean engaged_visit','mean visits')   --CR, ACBV, Orders, AOV and mean visits
  and platform in ('iOS','Android') -- only BOE experiments 
  and _date >= '2024-01-01'
  and additional_methodologies like ('%cuped%')
group by all 
qualify row_number() over (partition by experiment_id order by _date desc) = 1 -- last day of experiment 
)
select * from raw_data where experiment_id = 1305459088742
--pval 0.1227
--power 0.0588785610133
--%  5.4765461238
select
  a._date,
  a.experiment_id,
  b.name as experiment_name,
  b.team as squad,
  b.initiative,
  treatment,
  --overall results 
  case when metric_name in ('conversion rate') then p_value end as cr_pval,
  case when metric_name in ('conversion rate') then percent_change end as cr_percent_change,
  case when metric_name in ('conversion rate') then _power end as cr_power,
  
  case when metric_name in ('winsorized ac*v') then p_value end as acxv_pval,
  case when metric_name in ('winsorized ac*v') then percent_change end as acxv_percent_change,
  case when metric_name in ('winsorized ac*v') then _power end as acxv_power,
  -- tablet results
  case when is_tablet = 'tablet' and metric_name in ('conversion rate') then p_value end as tablet_cr_pval,
  case when is_tablet = 'tablet' and metric_name in ('conversion rate') then percent_change end as tablet_cr_percent_change,
  case when is_tablet = 'tablet' and metric_name in ('conversion rate') then _power end as tablet_cr_power,
  
  case when is_tablet = 'tablet' and metric_name in ('winsorized ac*v') then p_value end as tablet_acxv_pval,
  case when is_tablet = 'tablet' and metric_name in ('winsorized ac*v') then percent_change end as tablet_acxv_percent_change,
  case when is_tablet = 'tablet' and metric_name in ('winsorized ac*v') then _power end as tablet_acxv_power,
 
  -- not tablet results 
  case when is_tablet = 'not tablet' and metric_name in ('conversion rate') then p_value end as not_tablet_cr_pval,
  case when is_tablet = 'not tablet' and metric_name in ('conversion rate') then percent_change end as not_tablet_cr_percent_change,
  case when is_tablet = 'not tablet' and metric_name in ('conversion rate') then _power end as not_tablet_cr_power,
  
  case when is_tablet = 'not tablet' and metric_name in ('winsorized ac*v') then p_value end as not_tablet_acxv_pval,
  case when is_tablet = 'not tablet' and metric_name in ('winsorized ac*v') then percent_change end as not_tablet_acxv_percent_change,
  case when is_tablet = 'not tablet' and metric_name in ('winsorized ac*v') then _power end as not_tablet_acxv_power
from raw_data a
left join etsy-data-warehouse-prod.etsy_atlas.catapult_launches b
  on a.experiment_id=b.launch_id
