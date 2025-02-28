DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS Cities;
DROP TABLE IF EXISTS Employers;

CREATE TABLE Cities (
    CityID SERIAL PRIMARY KEY,
    CityName VARCHAR(100) UNIQUE NOT NULL,
    Country VARCHAR(100) NOT NULL
);

CREATE TABLE Employers (
    EmployerID SERIAL PRIMARY KEY,
    EmployerName VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE Customers (
    CustomerID SERIAL PRIMARY KEY,
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    Email VARCHAR(100),
    DateOfBirth DATE,
    CityID INT REFERENCES Cities(CityID) ON DELETE SET NULL,
    Country VARCHAR(100),
    Gender VARCHAR(10),
    NationalInsurance VARCHAR(100),
    EmployerID INT REFERENCES Employers(EmployerID) ON DELETE SET NULL
);

CREATE OR REPLACE FUNCTION InsertCustomers()
RETURNS VOID AS $$
DECLARE
    i INT := 1;
    city_name VARCHAR(100);
    employer_name VARCHAR(100);
    city_id INT;
    employer_id INT;
BEGIN
    FOR i IN 1..10 LOOP
        INSERT INTO Cities (CityName, Country)
        VALUES (CONCAT('City', i), 'United Kingdom')
        ON CONFLICT (CityName) DO NOTHING;
    END LOOP;

    FOR i IN 1..100 LOOP
        INSERT INTO Employers (EmployerName)
        VALUES (CONCAT('Company', i))
        ON CONFLICT (EmployerName) DO NOTHING;
    END LOOP;

    i := 1;
    WHILE i <= 100 LOOP
        city_name := CONCAT('City', (i % 10) + 1);
        employer_name := CONCAT('Company', i);

        SELECT CityID INTO city_id FROM Cities WHERE CityName = city_name;

        SELECT EmployerID INTO employer_id FROM Employers WHERE EmployerName = employer_name;

        INSERT INTO Customers (FirstName, LastName, Email, DateOfBirth, CityID, Country, Gender, NationalInsurance, EmployerID)
        VALUES (
            CONCAT('FirstName', i),
            CONCAT('LastName', i),
            CONCAT('person', i, '@gmail.com'),
            CURRENT_DATE - (i % 30) * INTERVAL '1 year',
            city_id,
            'United Kingdom',
            CASE WHEN i % 2 = 0 THEN 'Male' ELSE 'Female' END,
            CONCAT('FDJ010', i),
            employer_id
        );

        i := i + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT InsertCustomers();

SELECT 
    c.CustomerID, 
    c.FirstName, 
    c.LastName, 
    c.Email, 
    c.DateOfBirth, 
    ci.CityName, 
    ci.Country, 
    c.Gender, 
    c.NationalInsurance, 
    e.EmployerName
FROM Customers c
LEFT JOIN Cities ci ON c.CityID = ci.CityID
LEFT JOIN Employers e ON c.EmployerID = e.EmployerID;

SELECT ci.CityName, COUNT(c.CustomerID) AS CustomerCount
FROM Cities ci
LEFT JOIN Customers c ON ci.CityID = c.CityID
GROUP BY ci.CityName
ORDER BY CustomerCount DESC;

SELECT e.EmployerName, COUNT(c.CustomerID) AS EmployeeCount
FROM Employers e
LEFT JOIN Customers c ON e.EmployerID = c.EmployerID
GROUP BY e.EmployerName
ORDER BY EmployeeCount DESC
LIMIT 1;

SELECT FirstName, LastName, DateOfBirth
FROM Customers
WHERE DateOfBirth <= CURRENT_DATE - INTERVAL '50 years';

SELECT Gender, COUNT(*) AS Count
FROM Customers
GROUP BY Gender;

SELECT c.FirstName, c.LastName, e.EmployerName
FROM Customers c
JOIN Employers e ON c.EmployerID = e.EmployerID
WHERE e.EmployerName = 'Company5';

SELECT EmployerName 
FROM Employers 
WHERE EmployerID NOT IN (SELECT EmployerID FROM Customers);

SELECT ci.CityName, COUNT(c.CustomerID) AS FemaleCount
FROM Cities ci
JOIN Customers c ON ci.CityID = c.CityID
WHERE c.Gender = 'Female'
GROUP BY ci.CityName
ORDER BY FemaleCount DESC
LIMIT 1;

DELETE FROM Customers 
WHERE DateOfBirth <= CURRENT_DATE - INTERVAL '80 years';

UPDATE Customers
SET EmployerID = (SELECT EmployerID FROM Employers WHERE EmployerName = 'Company20')
WHERE FirstName = 'FirstName10' AND LastName = 'LastName10';

SELECT c1.FirstName AS Customer1, c1.LastName AS LastName1,
       c2.FirstName AS Customer2, c2.LastName AS LastName2,
       e.EmployerName
FROM Customers c1
JOIN Customers c2 ON c1.EmployerID = c2.EmployerID AND c1.CustomerID <> c2.CustomerID
JOIN Employers e ON c1.EmployerID = e.EmployerID
ORDER BY e.EmployerName;

SELECT FirstName, LastName, DateOfBirth,
       RANK() OVER (ORDER BY DateOfBirth ASC) AS AgeRank
FROM Customers;

