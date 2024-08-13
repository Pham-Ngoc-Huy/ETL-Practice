CREATE TABLE SalesDataLake (
    id INT IDENTITY(1,1) PRIMARY KEY,
    Date DATE,
    Product_ID INT,
    Store_ID INT,
    Units_Sold INT,
    Unit_Price DECIMAL(10, 2)
);
