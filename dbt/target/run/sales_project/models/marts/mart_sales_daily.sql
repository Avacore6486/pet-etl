
  
    
    
    

    
        create table `default_marts`.`mart_sales_daily__dbt_backup`
        
  
        
  engine = MergeTree()
        
      order by (tuple())
        
        
        
                    -- end_of_sql
                    
                    
          as (
            -- Витрина: агрегированные метрики продаж по дням/регионам/категориям

SELECT
    event_date,
    region,
    category,
    count()                                         AS total_orders,
    countDistinct(order_id)                         AS unique_orders,
    sum(quantity)                                   AS total_quantity,
    round(sum(total_price), 2)                      AS total_revenue,
    round(avg(total_price), 2)                      AS avg_order_value,
    round(avg(valid_rating), 2)                     AS avg_rating,
    countIf(is_returned = true)                     AS returned_orders,
    round(countIf(is_returned = true) / count() * 100, 2) AS return_rate_pct,
    countIf(has_discount = true)                    AS discounted_orders
FROM `default_staging`.`stg_sales_events`
GROUP BY event_date, region, category
ORDER BY event_date DESC, total_revenue DESC
          )
  