## 1. DATA PREPARATION

1.1 Перевірка базової цілісності
"""sql
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT Order_ID) AS unique_orders,
  COUNT(DISTINCT Customer_ID) AS unique_customers
FROM dataset.ecommerce;
"""
<img width="719" height="99" alt="1" src="https://github.com/user-attachments/assets/f56b218d-78d0-4495-be49-490aef2e6bba" />



