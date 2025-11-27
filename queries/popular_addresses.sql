-- ======================================================
-- Популярные адреса по количеству money flow
-- ======================================================

-- Вариант 1: Объединенный подсчет (from_address + to_address)
-- Показывает общее количество транзакций, где адрес участвовал как отправитель или получатель
SELECT 
    address,
    COUNT(*) as total_flows,
    SUM(CASE WHEN role = 'from' THEN 1 ELSE 0 END) as as_sender,
    SUM(CASE WHEN role = 'to' THEN 1 ELSE 0 END) as as_receiver
FROM (
    SELECT from_address as address, 'from' as role
    FROM xrpl.money_flow
    FINAL
    WHERE from_address != ''
    
    UNION ALL
    
    SELECT to_address as address, 'to' as role
    FROM xrpl.money_flow
    FINAL
    WHERE to_address != ''
)
GROUP BY address
ORDER BY total_flows DESC
LIMIT 100;

-- ======================================================
-- Вариант 2: Только как отправитель (from_address)
-- ======================================================
SELECT 
    from_address as address,
    COUNT(*) as flows_as_sender
FROM xrpl.money_flow
FINAL
WHERE from_address != ''
GROUP BY from_address
ORDER BY flows_as_sender DESC
LIMIT 100;

-- ======================================================
-- Вариант 3: Только как получатель (to_address)
-- ======================================================
SELECT 
    to_address as address,
    COUNT(*) as flows_as_receiver
FROM xrpl.money_flow
FINAL
WHERE to_address != ''
GROUP BY to_address
ORDER BY flows_as_receiver DESC
LIMIT 100;

-- ======================================================
-- Вариант 4: Детальная статистика по адресу
-- ======================================================
SELECT 
    address,
    total_flows,
    flows_as_sender,
    flows_as_receiver,
    ROUND(flows_as_sender * 100.0 / total_flows, 2) as sender_percentage,
    ROUND(flows_as_receiver * 100.0 / total_flows, 2) as receiver_percentage
FROM (
    SELECT 
        address,
        COUNT(*) as total_flows,
        SUM(CASE WHEN role = 'from' THEN 1 ELSE 0 END) as flows_as_sender,
        SUM(CASE WHEN role = 'to' THEN 1 ELSE 0 END) as flows_as_receiver
    FROM (
        SELECT from_address as address, 'from' as role
        FROM xrpl.money_flow
        FINAL
        WHERE from_address != ''
        
        UNION ALL
        
        SELECT to_address as address, 'to' as role
        FROM xrpl.money_flow
        FINAL
        WHERE to_address != ''
    )
    GROUP BY address
)
ORDER BY total_flows DESC
LIMIT 100;

-- ======================================================
-- Вариант 5: Без FINAL (быстрее, но может быть дубликаты из-за ReplacingMergeTree)
-- Используйте этот вариант если производительность важнее точности
-- ======================================================
SELECT 
    address,
    COUNT(*) as total_flows
FROM (
    SELECT from_address as address
    FROM xrpl.money_flow
    WHERE from_address != ''
    
    UNION ALL
    
    SELECT to_address as address
    FROM xrpl.money_flow
    WHERE to_address != ''
)
GROUP BY address
ORDER BY total_flows DESC
LIMIT 100;

