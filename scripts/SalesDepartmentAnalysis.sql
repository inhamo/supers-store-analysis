SELECT * 
FROM super_store.dbo.Orders;

-- Adding the Total Cost and Total Sales columns if they do not exist
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Orders' AND COLUMN_NAME = 'SalesAmount')
BEGIN
    ALTER TABLE super_store.dbo.Orders
    ADD SalesAmount DECIMAL(10,2);
END;

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Orders' AND COLUMN_NAME = 'TotalCost')
BEGIN
    ALTER TABLE super_store.dbo.Orders
    ADD TotalCost DECIMAL(10, 2);
END;

-- Updating the new columns
UPDATE super_store.dbo.Orders
SET SalesAmount = UnitPrice * Quantity;

UPDATE super_store.dbo.Orders
SET TotalCost = SalesAmount + ShippingCost;

-- Converting order and shipping dates to date format yyyy-mm-dd
UPDATE super_store.dbo.Orders
SET OrderDate = TRY_CONVERT(DATE, OrderDate, 103);

UPDATE super_store.dbo.Orders
SET ShipDate = TRY_CONVERT(DATE, ShipDate, 103);

-- Calculate Total Sales by year to have an overview of growth 
SELECT 
	YEAR(OrderDate) AS Year,
	SUM(SalesAmount) AS TotalSales,
	LAG(SUM(SalesAmount)) OVER(ORDER BY(YEAR(OrderDate))) As LaggedSales,
	((SUM(SalesAmount) / LAG(SUM(SalesAmount)) OVER(ORDER BY(YEAR(OrderDate)))) - 1) * 100 AS YoYTotalSales
FROM super_store.dbo.Orders
GROUP BY Year(OrderDate);

-- Calculate the CARG 
WITH YearlySales AS (
    SELECT 
        YEAR(OrderDate) AS Year,
        SUM(SalesAmount) AS TotalSales
    FROM 
        super_store.dbo.Orders
    GROUP BY 
        YEAR(OrderDate)
)
SELECT 
    (SELECT TotalSales FROM YearlySales WHERE Year = (SELECT MIN(Year) FROM YearlySales)) AS FirstYearSales,
    (SELECT TotalSales FROM YearlySales WHERE Year = (SELECT MAX(Year) FROM YearlySales)) AS LastYearSales,
    (SELECT MAX(Year) - MIN(Year) FROM YearlySales) AS NumberOfYears,
    -- Calculate CAGR
    POWER(
        (SELECT TotalSales FROM YearlySales WHERE Year = (SELECT MAX(Year) FROM YearlySales)) / 
        (SELECT TotalSales FROM YearlySales WHERE Year = (SELECT MIN(Year) FROM YearlySales)),
        1.0 / (SELECT MAX(Year) - MIN(Year) FROM YearlySales)
    ) - 1 AS CAGR


/* INVESTIGATE THE GROWTH IN SALES REVENUE IF THERE IS GROWTH */
-- CASE : WHAT IF THE SALE REVENUE GROWTH WAS DUE TO THE NEW PRODUCTS 
DECLARE @MostRecentYear INT;

-- Get the most recent year
SELECT @MostRecentYear = MAX(YEAR(OrderDate))
FROM super_store.dbo.Orders;

WITH ProductList AS (
    SELECT 
        ProductName, 
        MIN(YEAR(OrderDate)) AS FirstProductSale
    FROM super_store.dbo.Orders
    GROUP BY ProductName
),
ExistingProducts AS (
    -- Select products that are not new
    SELECT ProductName
    FROM ProductList
    WHERE FirstProductSale < @MostRecentYear
)
-- Calculate total sales this year excluding the new products
SELECT 
    SUM(o.SalesAmount) AS TotalSalesExcludingNewProducts,
    -- Calculate YoY sales growth: Compare this year's total sales with last year's
    ((SUM(o.SalesAmount) / 
        (SELECT SUM(SalesAmount)
         FROM super_store.dbo.Orders o2
         JOIN ExistingProducts e2 ON o2.ProductName = e2.ProductName
         WHERE YEAR(o2.OrderDate) = @MostRecentYear - 1)) - 1) * 100 AS YoYTotalSales
FROM super_store.dbo.Orders o
JOIN ExistingProducts e ON o.ProductName = e.ProductName
WHERE YEAR(o.OrderDate) = @MostRecentYear;


-- CASE : WHAT IF THE SALE REVENUE GROWTH WAS DUE TO THE NEW CUSTOMERS 
DECLARE @MostRecentYear INT;

-- Get the most recent year
SELECT @MostRecentYear = MAX(YEAR(OrderDate))
FROM super_store.dbo.Orders;

WITH CustomerList AS (
    SELECT 
        CustomerID, 
        MIN(YEAR(OrderDate)) AS FirstCustomerPurchase
    FROM super_store.dbo.Orders
    GROUP BY CustomerID
),
ExistingCustomers AS (
    -- Select customers who were already purchasing before the most recent year
    SELECT CustomerID
    FROM CustomerList
    WHERE FirstCustomerPurchase < @MostRecentYear
),
LastYearSales AS (
    -- Get total sales from existing customers in the previous year
    SELECT 
        SUM(o.SalesAmount) AS TotalSalesLastYear
    FROM super_store.dbo.Orders o
    JOIN ExistingCustomers e ON o.CustomerID = e.CustomerID
    WHERE YEAR(o.OrderDate) = @MostRecentYear - 1
)
-- Calculate total sales this year excluding new customers
SELECT 
    SUM(o.SalesAmount) AS TotalSalesExcludingNewCustomers,
    -- Calculate YoY sales growth while handling division by zero
    CASE 
        WHEN (SELECT TotalSalesLastYear FROM LastYearSales) = 0 THEN NULL
        ELSE ((SUM(o.SalesAmount) / (SELECT TotalSalesLastYear FROM LastYearSales)) - 1) * 100 
    END AS YoYTotalSales
FROM super_store.dbo.Orders o
JOIN ExistingCustomers e ON o.CustomerID = e.CustomerID
WHERE YEAR(o.OrderDate) = @MostRecentYear;


-- CASE 3 : OLD PRODUCTS
DECLARE @MostRecentYear INT;

-- Get the most recent year
SELECT @MostRecentYear = MAX(YEAR(OrderDate))
FROM super_store.dbo.Orders;

WITH CustomerList AS (
    -- Find the first purchase year for each customer
    SELECT 
        CustomerID, 
        MIN(YEAR(OrderDate)) AS FirstCustomerPurchase
    FROM super_store.dbo.Orders
    GROUP BY CustomerID
),
ExistingCustomers AS (
    -- Customers who made purchases before the most recent year
    SELECT CustomerID
    FROM CustomerList
    WHERE FirstCustomerPurchase < @MostRecentYear
),
ProductList AS (
    -- Find the first year each product was sold
    SELECT 
        ProductName, 
        MIN(YEAR(OrderDate)) AS FirstProductSale
    FROM super_store.dbo.Orders
    GROUP BY ProductName
),
OldProducts AS (
    -- Products that were sold before the most recent year
    SELECT ProductName
    FROM ProductList
    WHERE FirstProductSale < @MostRecentYear
),
CustomerDemand AS (
    -- Calculate total quantity purchased per customer per year
    SELECT 
        o.CustomerID,
        YEAR(o.OrderDate) AS OrderYear,
        SUM(o.Quantity) AS TotalQuantity,
        SUM(CASE WHEN op.ProductName IS NOT NULL THEN o.Quantity ELSE 0 END) AS OldProductQuantity,
        SUM(CASE WHEN op.ProductName IS NULL THEN o.Quantity ELSE 0 END) AS NewProductQuantity
    FROM super_store.dbo.Orders o
    LEFT JOIN OldProducts op ON o.ProductName = op.ProductName
    JOIN ExistingCustomers ec ON o.CustomerID = ec.CustomerID
    GROUP BY o.CustomerID, YEAR(o.OrderDate)
)
-- Compare quantity demand for old vs. new products
SELECT 
    cd1.CustomerID,
    cd1.OrderYear AS LastYear,
    cd1.TotalQuantity AS LastYearQuantity,
    cd2.OrderYear AS ThisYear,
    cd2.TotalQuantity AS ThisYearQuantity,
    
    -- Growth in overall quantity per customer
    ((cd2.TotalQuantity - cd1.TotalQuantity) / NULLIF(cd1.TotalQuantity, 0)) * 100 AS TotalQuantityGrowth,
    
    -- Growth in old products per customer
    cd1.OldProductQuantity AS QuantityOldProductsLastYear,
    cd2.OldProductQuantity AS QuantityOldProductsThisYear,
    ((cd2.OldProductQuantity - cd1.OldProductQuantity) / NULLIF(cd1.OldProductQuantity, 0)) * 100 AS OldProductGrowth

FROM CustomerDemand cd1
JOIN CustomerDemand cd2 
    ON cd1.CustomerID = cd2.CustomerID
    AND cd1.OrderYear = @MostRecentYear - 1 
    AND cd2.OrderYear = @MostRecentYear;



-- WHAT HAPPENED TO THE OLD CUSTOMERS DESPITE ACQUIRING THE NEW CUSTOMERS
DECLARE @MostRecentYear INT;

-- Get the most recent year
SELECT @MostRecentYear = MAX(YEAR(OrderDate))
FROM super_store.dbo.Orders;

WITH CustomerList AS (
    SELECT 
        CustomerID, 
        MIN(YEAR(OrderDate)) AS FirstCustomerPurchase
    FROM super_store.dbo.Orders
    GROUP BY CustomerID
),
ExistingCustomers AS (
    -- Customers who were already purchasing before the most recent year
    SELECT CustomerID
    FROM CustomerList
    WHERE FirstCustomerPurchase < @MostRecentYear
),
CustomerSales AS (
    -- Get total sales per customer per year
    SELECT 
        o.CustomerID,
        YEAR(o.OrderDate) AS OrderYear,
        SUM(o.SalesAmount) AS TotalSales
    FROM super_store.dbo.Orders o
    JOIN ExistingCustomers ec ON o.CustomerID = ec.CustomerID
    GROUP BY o.CustomerID, YEAR(o.OrderDate)
)
-- Compare sales year-over-year
SELECT 
    cs1.CustomerID,
    cs1.TotalSales AS LastYearSales,
    cs2.TotalSales AS ThisYearSales,
    ((cs2.TotalSales - cs1.TotalSales) / NULLIF(cs1.TotalSales, 0)) * 100 AS SalesGrowth
FROM CustomerSales cs1
JOIN CustomerSales cs2 ON cs1.CustomerID = cs2.CustomerID
WHERE cs1.OrderYear = @MostRecentYear - 1 
AND cs2.OrderYear = @MostRecentYear;
