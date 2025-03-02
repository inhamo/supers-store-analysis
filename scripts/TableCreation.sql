USE super_store;

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Orders')
BEGIN
    CREATE TABLE Orders (
        OrderPriority VARCHAR(50),
        UnitPrice DECIMAL(10,2),
        ShippingCost DECIMAL(10,2),
        CustomerID VARCHAR(50),
        CustomerName VARCHAR(255),
        ShipMode VARCHAR(50),
        CustomerSegment VARCHAR(50),
        ProductCategory VARCHAR(50),
        ProductSubCategory VARCHAR(50),
        ProductContainer VARCHAR(50),
        ProductName VARCHAR(255),
        Region VARCHAR(50),
        StateOrProvince VARCHAR(50),
        City VARCHAR(100),
        OrderDate VARCHAR(20),
        ShipDate VARCHAR(20),
        Quantity INT,
        OrderID VARCHAR(50) 
    );
END;

-- import data into the table 
BULK INSERT Orders
FROM 'C:\Users\takue\Documents\Data Analysis\Retail Data\Sample Superstore - Orders.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

SELECT * 
FROM super_store.dbo.Orders;