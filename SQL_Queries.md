## 1. DATA PREPARATION

**1.1 Перевірка базової цілісності**

```sql
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT Order_ID) AS unique_orders,
  COUNT(DISTINCT Customer_ID) AS unique_customers
FROM dataset.ecommerce;
```
<img width="719" height="99" alt="1" src="https://github.com/user-attachments/assets/f56b218d-78d0-4495-be49-490aef2e6bba" />


**1.2 Додаткові поля**

Створюю view для аналізу, додаю колонки для дослідження часових патернів 

```sql
CREATE OR REPLACE VIEW dataset.v_cleaned AS
SELECT
  *,
  DATE_TRUNC(Date, MONTH) AS order_month,
  EXTRACT(YEAR FROM Date) AS order_year,
  EXTRACT(MONTH FROM Date) AS order_month_num
FROM dataset.ecommerce;
```

## 2. REVENUE ANALYTICS

**2.1 Total Revenue & Average Order Value (AOV)**

```sql
SELECT 
    ROUND(SUM(Total_Amount),2) AS total_revenue,
    ROUND(AVG(Total_Amount),2) AS avg_order_value
FROM dataset.v_cleaned;
```
<img width="897" height="70" alt="3" src="https://github.com/user-attachments/assets/2e3b776b-92eb-4249-9918-e73f56ee293e" />


**2.2 Revenue by Category**

```sql
SELECT
  Product_Category,
  COUNT(*) AS orders,
  ROUND(SUM(Total_Amount), 2) AS revenue,
  ROUND(AVG(Total_Amount), 2) AS avg_order_value
FROM dataset.v_cleaned
GROUP BY Product_Category
ORDER BY revenue DESC;
```
<img width="796" height="308" alt="4" src="https://github.com/user-attachments/assets/1c681053-32f0-4930-b7c9-19eae3bfe7ec" />


**2.3 Revenue by City**

```sql
SELECT
  City,
  COUNT(*) AS orders,
  ROUND(SUM(Total_Amount), 2) AS revenue,
  ROUND(AVG(Total_Amount), 2) AS avg_order_value
FROM dataset.v_cleaned
GROUP BY City
ORDER BY revenue DESC;
```
<img width="796" height="373" alt="5" src="https://github.com/user-attachments/assets/b4f577cb-ab1c-4b39-94d7-8d437931d97d" />


**2.4 Revenue by Device**

```sql
SELECT
	Device_Type,
	COUNT(*) AS orders,
	ROUND(SUM(Total_Amount), 2) AS revenue,
	ROUND(AVG(Total_Amount), 2) AS avg_order_value
FROM dataset.v_cleaned
GROUP BY Device_Type
ORDER BY revenue DESC;
```
<img width="812" height="144" alt="6" src="https://github.com/user-attachments/assets/d9a072a1-18e0-4ec4-9e19-d7b7a24ed935" />


**2.5 Revenue by Gender**

```sql
SELECT
  Gender,
  COUNT(*) AS orders,
  ROUND(SUM(Total_Amount), 2) AS revenue,
  ROUND(AVG(Total_Amount), 2) AS avg_order_value
FROM dataset.v_cleaned
GROUP BY Gender
ORDER BY revenue DESC;
```
<img width="794" height="138" alt="7" src="https://github.com/user-attachments/assets/5f32d94d-1fd5-4c11-9fac-18ed7ccf336e" />


**2.6 Monthly Revenue & ARPU**

```sql
SELECT
  order_month,
  ROUND(SUM(Total_Amount), 2) AS monthly_revenue,
  ROUND(SUM(Total_Amount) / COUNT(DISTINCT Customer_ID), 2) AS arpu
FROM dataset.v_cleaned
GROUP BY order_month
ORDER BY order_month;
```
<img width="819" height="544" alt="8" src="https://github.com/user-attachments/assets/d18fb1c2-744f-45a1-aa2d-4208845d4d65" />


## 3. CUSTOMER ANALYTICS


















