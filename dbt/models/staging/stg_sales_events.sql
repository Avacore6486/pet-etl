-- Staging слой: базовая очистка и типизация сырых данных

SELECT
    id,
    order_id,
    customer_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    discount_pct,
    total_price,
    region,
    city,
    payment_method,
    channel,
    is_returned,
    rating,
    event_time,
    event_date,
    has_discount,
    -- очищаем невалидные рейтинги
    CASE WHEN rating BETWEEN 1 AND 5 THEN rating ELSE NULL END AS valid_rating
FROM {{ source('default', 'raw_sales_events') }}
WHERE cdc_op IN ('c', 'r')  -- только insert/snapshot, без delete
