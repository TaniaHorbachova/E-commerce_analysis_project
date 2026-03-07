# 1. DATA PREPARATION

## 1.1. Перевірка базової цілісності

```sql
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT Order_ID) AS unique_orders,
  COUNT(DISTINCT Customer_ID) AS unique_customers
FROM dataset.ecommerce;
```
<img width="719" height="99" alt="1" src="https://github.com/user-attachments/assets/f56b218d-78d0-4495-be49-490aef2e6bba" />

**Висновки:**

Результат запиту підтверджує, що 1 рядок таблиці = 1 транзакції.
Кількість унікальних користувачів в цій таблиці - 5000.


## 1.2. Додаткові поля

Створюю таблицю для аналізу, додаю колонки для дослідження часових патернів 

```sql
CREATE OR REPLACE VIEW dataset.v_cleaned AS
SELECT
  *,
  DATE_TRUNC(Date, MONTH) AS order_month,
  EXTRACT(YEAR FROM Date) AS order_year,
  EXTRACT(MONTH FROM Date) AS order_month_num
FROM dataset.ecommerce;
```


# 2. REVENUE ANALYTICS

## 2.1. Total Revenue & Average Order Value (AOV)

```sql
SELECT 
    ROUND(SUM(Total_Amount),2) AS total_revenue,
    ROUND(AVG(Total_Amount),2) AS avg_order_value
FROM dataset.v_cleaned;
```
<img width="897" height="70" alt="3" src="https://github.com/user-attachments/assets/2e3b776b-92eb-4249-9918-e73f56ee293e" />

**Висновки:**

Загальна виручка склала 21.78 млн, середня вартість замовлення становить 1277.44. 


## 2.2. Revenue by Category

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

**Висновки:**

Electronics є безумовним лідером за виручкою - 10.48М, що у 2,5 рази перевищує наступну категорію - Home & Garden. Категорія Books має найменший внесок у загальний дохід.

## 2.3. Revenue by City

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

**Висновки:**

Istanbul генерує найбільший обсяг продажів - 5.65М та замовлень - 4402. Міста Ankara та Izmir замикають трійку лідерів, що показує зосередженість великих ринків в мегаполісах.


## 2.4. Revenue by Device

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

**Висновки:**

Категорія Mobile є пріоритетним каналом продажів - через неї проходить понад 12М виручки, саме для цієї категорії треба спрямовувати бюджети на стратегії для утримання клієнтів.


## 2.5. Revenue by Gender

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

**Висновки:**

Жіноча аудиторія демонструє вищу активність та приносить більше доходу - 11.04М, хоча середня вартість замовлення у категорії "Other" є вищою - 1530.32.



## 2.6. Monthly Revenue & ARPU

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

**Висновки:**

Пік виручки зафіксовано у грудні 2023 року - 1.58М, що корелює з сезонними святами. Починаючи з січня 2024 року, спостерігається тренд до зниження доходів.


# 3. CUSTOMER ANALYTICS

## 3.1. Customer-Level Aggregation

Створюю таблицю для подальшого аналізу RFM та аналізу відтоку в Python-середовищі. Клієнт вважається таким, що покинув сервіс, якщо він не здійснював покупок більше **90 днів** відносно останньої дати в наборі даних.

Поріг churn встановлено на рівні 90 днів неактивності, бо це точка, де клієнт вже відхиляється від типової поведінки, але ще може бути повернений. Середній інтервал між покупками становить 124 дні, однак використання середнього як порогу є некоректним через необхідність раннього виявлення ризику. Також це значення порогу узгоджується з практиками e‑commerce.


```sql
CREATE OR REPLACE TABLE dataset.v_customer_churn AS

WITH customer_base AS (
  SELECT
    Customer_ID,
    MIN(Date) AS first_purchase,
    MAX(Date) AS last_purchase,
    COUNT(Order_ID) AS frequency,
    SUM(Total_Amount) AS monetary
  FROM dataset.v_cleaned
  GROUP BY Customer_ID
),

reference_date AS (
  SELECT
    MAX(Date) AS max_date
  FROM dataset.v_cleaned
)

SELECT
  c.Customer_ID,
  c.first_purchase,
  c.last_purchase,
  c.frequency,
  c.monetary,
  DATE_DIFF(c.last_purchase, c.first_purchase, DAY) AS lifetime_days,
  DATE_DIFF(r.max_date, c.last_purchase, DAY) AS recency,

  CASE 
    WHEN DATE_DIFF(r.max_date, c.last_purchase, DAY) > 90 THEN 1
    ELSE 0
  END AS churn

FROM customer_base c
CROSS JOIN reference_date r;
```

## 3.2. Перевірка Churn Rate

```sql
SELECT 
  churn,
  COUNT(*) AS customers,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS rate
FROM dataset.v_customer_churn
GROUP BY churn;
```
<img width="865" height="103" alt="9" src="https://github.com/user-attachments/assets/c900ff79-3dbd-4d11-b372-0cd55ef1585c" />

**Висновки:**

Коефіцієнт відтоку клієнтів складає 51%. Це критичний показник, який сигналізує про необхідність термінового впровадження Retention-стратегій для стабілізації клієнтської бази.

# 4. BEHAVIORAL ANALYTICS

## 4.1. Session Duration vs Spend

Додаю кількість сесій у кожний сегмент, для розуміння значущості, бо якщо в сегменті "30 хвилин" всього 2 клієнта, то цей високий чек - просто випадковість.

```sql
SELECT
  ROUND(Session_Duration_Minutes, -1) AS session_bucket,
  ROUND(AVG(Total_Amount), 2) AS avg_spend,
  COUNT(*) AS session_count, 
  ROUND(STDDEV(Total_Amount), 2) AS price_variability --  розкид цін
FROM dataset.v_cleaned
GROUP BY session_bucket
ORDER BY session_bucket;
```
<img width="706" height="173" alt="10" src="https://github.com/user-attachments/assets/5a81fd82-3488-42cc-9a66-807d70e5283a" />

**Висновки:**

Сесій тривалістю до 10 хвилин надзвичайно мало - 2, як і сесій тривалістю від 20 до 30 хвилин - всього 8, але цей бакет демонструє аномально високий середній чек - 2587.24, що майже вдвічі перевищує середні показники інших груп.

Користувачі в категорії від 10 до 20 хвилин мають найвищу активність - понад 17 000 сесій, що є основним джерелом стабільного трафіку.

## 4.2. Pages Viewed vs Spend

```sql
SELECT
  ROUND(Pages_Viewed, -1) AS pages_bucket,
  ROUND(AVG(Total_Amount), 2) AS avg_spend,
  COUNT(*) AS session_count, -- кількість сторінок для перевірки значущості
  ROUND(STDDEV(Total_Amount), 2) AS price_variability 
FROM dataset.v_cleaned
GROUP BY pages_bucket
ORDER BY pages_bucket;
```
<img width="711" height="145" alt="11" src="https://github.com/user-attachments/assets/ecba6108-b7a3-430f-942b-a5c5d8a76388" />

**Висновки:**

Користувачі в бакеті 10 демонструють найвищу залученість - 16 531 сесія та найвищий середній чек - 1281.04.

При переході до бакету 20 активність падає майже в 140 разів, що може вказувати на технічні труднощі під час довгих сесій або втрату інтересу.


## 4.3. Delivery Time vs Rating

```sql
SELECT
  Delivery_Time_Days,
  ROUND(AVG(Customer_Rating), 2) AS avg_rating,
  COUNT(*) AS orders
FROM dataset.v_cleaned
GROUP BY Delivery_Time_Days
ORDER BY Delivery_Time_Days;
```
<img width="545" height="373" alt="12 1" src="https://github.com/user-attachments/assets/f620d67a-2cc0-4986-b79c-c80460044ded" />
<img width="545" height="378" alt="12 2" src="https://github.com/user-attachments/assets/1366e2fa-5dcb-4293-922b-ef8fc8771969" />
<img width="549" height="142" alt="12 3" src="https://github.com/user-attachments/assets/5bbe939f-6537-4c00-b516-f9cd1b0580af" />

**Висновки:**

Середній рейтинг клієнтів залишається відносно стабільним (близько 3,8-3,9) для термінів доставки від 2 до 15 днів, на які припадає більшість замовлень. Для довших термінів доставки - понад 16 днів, кількість замовлень значно падає, що робить рейтинги менш надійними через невеликий розмір вибірки. В цілому, термін доставки не має сильного впливу на рейтинги клієнтів у найпоширенішому діапазоні доставки.


## 4.4. Returning vs New Customers

```sql
SELECT
  Is_Returning_Customer,
  COUNT(*) AS orders,
  ROUND(AVG(Total_Amount), 2) AS avg_spend
FROM dataset.v_cleaned
GROUP BY Is_Returning_Customer;
```
<img width="515" height="108" alt="13" src="https://github.com/user-attachments/assets/61763111-f8b5-4400-8290-f0cf94c92d7a" />

**Висновки:**

Постійні клієнти генерують у 7,5 разів більше замовлень, ніж нові.

Нові клієнти мають трохи вищий середній чек - 1287.73 проти 1276.06, але стабільність доходу забезпечує саме повернення аудиторії.



