SET search_path TO star, public;

-- A1. Monthly report: one row per store and calendar month
SELECT
    d.CalendarYear,
    d.MonthNumber,
    s.StoreID,
    s.StoreName,
    SUM(f.SalesAmount) AS RevenueEUR
FROM star.FactSales AS f
JOIN star.DimDate AS d ON f.DateKey = d.DateKey
JOIN star.DimStore AS s ON f.StoreKey = s.StoreKey
GROUP BY d.CalendarYear, d.MonthNumber, s.StoreID, s.StoreName
ORDER BY d.CalendarYear, d.MonthNumber, s.StoreID;

-- A2. Daily report: one row per store and date
SELECT
    d.FullDate,
    s.StoreID,
    s.StoreName,
    SUM(f.SalesAmount) AS RevenueEUR
FROM star.FactSales AS f
JOIN star.DimDate AS d ON f.DateKey = d.DateKey
JOIN star.DimStore AS s ON f.StoreKey = s.StoreKey
GROUP BY d.FullDate, s.StoreID, s.StoreName
ORDER BY d.FullDate, s.StoreID;