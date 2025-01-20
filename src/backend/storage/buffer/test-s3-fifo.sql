DROP TABLE IF EXISTS Customers;

CREATE TABLE Customers (
    CustomerID SERIAL PRIMARY KEY,
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    Email VARCHAR(100),
    DateOfBirth DATE,
    City VARCHAR(100),
    Country VARCHAR(100),
    Gender VARCHAR(10),
    NationalInsurance VARCHAR(100)
);

CREATE OR REPLACE FUNCTION InsertCustomers()
RETURNS VOID AS $$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= 1000 LOOP
        INSERT INTO Customers (FirstName, LastName, Email, DateOfBirth, City, Country, Gender, NationalInsurance)
        VALUES (
            CONCAT('FirstName', i),
            CONCAT('LastName', i),
            CONCAT('person', i, '@gmail.com'),
            CURRENT_DATE - (i % 30) * INTERVAL '1 year',
            CONCAT('City', (i % 10)),
            'United Kingdom',
            CASE WHEN i % 2 = 0 THEN 'Male' ELSE 'Female' END,
            CONCAT('FDJ010', i)
        );
        i := i + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT InsertCustomers();

SELECT * FROM Customers;

SELECT * FROM Customers WHERE Country = 'United Kingdom' AND Gender = 'Male';

SELECT CustomerID, FirstName, LastName, NationalInsurance FROM Customers WHERE City = 'City 9';

UPDATE Customers
SET NationalInsurance = 'FDJ0103'
WHERE City = 'City6';

DELETE FROM Customers WHERE EXTRACT(YEAR FROM DateOfBirth) < 1995;

SELECT City, COUNT(*) AS TotalCustomers
FROM Customers
GROUP BY City;

SELECT Gender, AVG(EXTRACT(YEAR FROM AGE(DateOfBirth))) AS AvgAge
FROM Customers
GROUP BY Gender;

SELECT *
FROM Customers
WHERE EXTRACT(YEAR FROM AGE(DateOfBirth)) BETWEEN 30 AND 50
    AND City = 'City 4';

SELECT *
FROM Customers
ORDER BY CustomerID DESC
LIMIT 5;
