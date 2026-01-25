# KPI Catalog

> **Synthetic demo output for portfolio purposes.**
> This catalog demonstrates standardized KPI definitions for e-commerce analytics.

---

## Overview

This catalog defines the Key Performance Indicators (KPIs) computed by the analytics pipeline. Each KPI includes:

- **Definition**: What it measures
- **Grain**: The level of aggregation
- **Formula**: Precise calculation method
- **Required Fields**: Data dependencies
- **Caveats**: Edge cases and limitations

---

## Revenue & Order KPIs

### KPI-001: Total Revenue

| Attribute | Value |
|-----------|-------|
| **Definition** | Sum of all completed order totals in the reporting period |
| **Grain** | Daily / Weekly / Monthly / Yearly |
| **Formula** | `SUM(order_total) WHERE order_status = 'Completed'` |
| **Required Fields** | `order_total`, `order_status`, `order_date` |
| **Unit** | Currency (USD) |
| **Caveats** | Excludes cancelled/refunded orders; includes taxes and fees |

---

### KPI-002: Total Orders

| Attribute | Value |
|-----------|-------|
| **Definition** | Count of distinct completed orders |
| **Grain** | Daily / Weekly / Monthly / Yearly |
| **Formula** | `COUNT(DISTINCT order_id) WHERE order_status = 'Completed'` |
| **Required Fields** | `order_id`, `order_status`, `order_date` |
| **Unit** | Integer |
| **Caveats** | Each order counted once regardless of line items |

---

### KPI-003: Average Order Value (AOV)

| Attribute | Value |
|-----------|-------|
| **Definition** | Mean revenue per completed order |
| **Grain** | Daily / Weekly / Monthly / Yearly |
| **Formula** | `SUM(order_total) / COUNT(DISTINCT order_id) WHERE order_status = 'Completed'` |
| **Required Fields** | `order_total`, `order_id`, `order_status` |
| **Unit** | Currency (USD) |
| **Caveats** | Sensitive to outliers; consider median for luxury goods |

---

### KPI-004: Revenue by Category

| Attribute | Value |
|-----------|-------|
| **Definition** | Revenue breakdown by product category |
| **Grain** | Category × Period |
| **Formula** | `SUM(order_total) GROUP BY product_category WHERE order_status = 'Completed'` |
| **Required Fields** | `order_total`, `product_category`, `order_status` |
| **Unit** | Currency (USD) per category |
| **Caveats** | Multi-item orders split by line-item subtotal if available |

---

### KPI-005: Revenue by Channel

| Attribute | Value |
|-----------|-------|
| **Definition** | Revenue attributed to each sales/marketing channel |
| **Grain** | Channel × Period |
| **Formula** | `SUM(order_total) GROUP BY sales_channel` or `marketing_channel` |
| **Required Fields** | `order_total`, `sales_channel` or `marketing_channel` |
| **Unit** | Currency (USD) per channel |
| **Caveats** | Attribution model: last-touch (unless specified otherwise) |

---

## Customer KPIs

### KPI-006: Unique Customers

| Attribute | Value |
|-----------|-------|
| **Definition** | Count of distinct customers with completed orders |
| **Grain** | Period |
| **Formula** | `COUNT(DISTINCT customer_id) WHERE order_status = 'Completed'` |
| **Required Fields** | `customer_id`, `order_status`, `order_date` |
| **Unit** | Integer |
| **Caveats** | Requires consistent customer_id across transactions |

---

### KPI-007: Customer Segment Distribution

| Attribute | Value |
|-----------|-------|
| **Definition** | Breakdown of customers and revenue by segment |
| **Grain** | Segment × Period |
| **Formula** | `COUNT(DISTINCT customer_id) GROUP BY customer_segment` |
| **Required Fields** | `customer_id`, `customer_segment` |
| **Unit** | Count and % |
| **Caveats** | Segments: VIP, Premium, Standard, New |

---

### KPI-008: Repeat Purchase Rate

| Attribute | Value |
|-----------|-------|
| **Definition** | Percentage of customers with >1 order in the period |
| **Grain** | Period |
| **Formula** | `COUNT(customers with ≥2 orders) / COUNT(DISTINCT customer_id) × 100` |
| **Required Fields** | `customer_id`, `order_id`, `order_date` |
| **Unit** | Percentage |
| **Caveats** | Period-sensitive; 12-month window recommended |

---

### KPI-009: Customer Lifetime Value (CLV) - Simplified

| Attribute | Value |
|-----------|-------|
| **Definition** | Average total revenue per customer over their relationship |
| **Grain** | Cohort or Overall |
| **Formula** | `SUM(order_total) / COUNT(DISTINCT customer_id)` |
| **Required Fields** | `customer_id`, `order_total` |
| **Unit** | Currency (USD) |
| **Caveats** | Simplified; production CLV uses predictive models |

---

### KPI-010: Customer Acquisition by Channel

| Attribute | Value |
|-----------|-------|
| **Definition** | New customers attributed to each marketing channel |
| **Grain** | Channel × Period |
| **Formula** | `COUNT(DISTINCT customer_id) WHERE first_order = TRUE GROUP BY marketing_channel` |
| **Required Fields** | `customer_id`, `marketing_channel`, `order_date` |
| **Unit** | Count per channel |
| **Caveats** | Requires identifying first-time purchasers |

---

## Product KPIs

### KPI-011: Units Sold

| Attribute | Value |
|-----------|-------|
| **Definition** | Total quantity of items sold |
| **Grain** | Period / Category / SKU |
| **Formula** | `SUM(quantity) WHERE order_status = 'Completed'` |
| **Required Fields** | `quantity`, `order_status` |
| **Unit** | Integer |
| **Caveats** | Counts items, not orders |

---

### KPI-012: Top Products by Revenue

| Attribute | Value |
|-----------|-------|
| **Definition** | Ranked list of SKUs/categories by revenue contribution |
| **Grain** | SKU or Category |
| **Formula** | `SUM(subtotal) GROUP BY sku ORDER BY revenue DESC LIMIT N` |
| **Required Fields** | `sku`, `subtotal`, `order_status` |
| **Unit** | Currency (USD) ranked |
| **Caveats** | Use subtotal (pre-discount) or net revenue as needed |

---

### KPI-013: Category Mix

| Attribute | Value |
|-----------|-------|
| **Definition** | Percentage contribution of each category to total revenue |
| **Grain** | Category |
| **Formula** | `SUM(subtotal per category) / SUM(total subtotal) × 100` |
| **Required Fields** | `product_category`, `subtotal` |
| **Unit** | Percentage |
| **Caveats** | Should sum to 100% across categories |

---

### KPI-014: Return Rate

| Attribute | Value |
|-----------|-------|
| **Definition** | Percentage of orders that were refunded/returned |
| **Grain** | Period / Category |
| **Formula** | `COUNT(orders WHERE status='Refunded') / COUNT(all orders) × 100` |
| **Required Fields** | `order_id`, `order_status` |
| **Unit** | Percentage |
| **Caveats** | Track by category to identify quality issues |

---

## Operational KPIs

### KPI-015: Discount Utilization Rate

| Attribute | Value |
|-----------|-------|
| **Definition** | Percentage of orders using discounts |
| **Grain** | Period |
| **Formula** | `COUNT(orders WHERE discount_amount > 0) / COUNT(all orders) × 100` |
| **Required Fields** | `discount_amount`, `order_id` |
| **Unit** | Percentage |
| **Caveats** | High rates may indicate pricing strategy issues |

---

### KPI-016: Average Discount Percentage

| Attribute | Value |
|-----------|-------|
| **Definition** | Mean discount as percentage of subtotal |
| **Grain** | Period / Segment |
| **Formula** | `AVG(discount_percent) WHERE discount_amount > 0` |
| **Required Fields** | `discount_percent` |
| **Unit** | Percentage |
| **Caveats** | Exclude zero-discount orders from average |

---

### KPI-017: Gift Order Rate

| Attribute | Value |
|-----------|-------|
| **Definition** | Percentage of orders marked as gifts |
| **Grain** | Period |
| **Formula** | `COUNT(orders WHERE is_gift = TRUE) / COUNT(all orders) × 100` |
| **Required Fields** | `is_gift` |
| **Unit** | Percentage |
| **Caveats** | Seasonal spikes expected around holidays |

---

## Sustainability KPIs

### KPI-018: Carbon per Order

| Attribute | Value |
|-----------|-------|
| **Definition** | Average carbon emissions per completed order |
| **Grain** | Period |
| **Formula** | `SUM(carbon_emissions_kg) / COUNT(DISTINCT order_id)` |
| **Required Fields** | `carbon_emissions_kg`, `order_id` |
| **Unit** | kg CO₂e |
| **Caveats** | Requires emissions data from logistics/supply chain |

---

### KPI-019: Sustainable Packaging Rate

| Attribute | Value |
|-----------|-------|
| **Definition** | Percentage of orders using sustainable packaging |
| **Grain** | Period |
| **Formula** | `COUNT(orders with sustainable_packaging = TRUE) / COUNT(all orders) × 100` |
| **Required Fields** | `sustainable_packaging` |
| **Unit** | Percentage |
| **Caveats** | Definition of "sustainable" should be documented |

---

### KPI-020: Energy Intensity

| Attribute | Value |
|-----------|-------|
| **Definition** | Energy consumed per dollar of revenue |
| **Grain** | Period |
| **Formula** | `SUM(energy_kwh) / SUM(order_total)` |
| **Required Fields** | `energy_kwh`, `order_total` |
| **Unit** | kWh / USD |
| **Caveats** | Lower is better; track trend over time |

---

## KPI Computation Matrix

| KPI ID | Name | Aggregation | Dimensions | Trending |
|--------|------|-------------|------------|----------|
| KPI-001 | Total Revenue | SUM | Time, Region | ✓ |
| KPI-002 | Total Orders | COUNT | Time, Region | ✓ |
| KPI-003 | AOV | RATIO | Time, Segment | ✓ |
| KPI-004 | Revenue by Category | SUM | Category, Time | ✓ |
| KPI-005 | Revenue by Channel | SUM | Channel, Time | ✓ |
| KPI-006 | Unique Customers | COUNT DISTINCT | Time | ✓ |
| KPI-007 | Segment Distribution | COUNT | Segment | ✓ |
| KPI-008 | Repeat Rate | RATIO | Time | ✓ |
| KPI-009 | CLV | RATIO | Cohort | ✓ |
| KPI-010 | Acquisition by Channel | COUNT | Channel | ✓ |
| KPI-011 | Units Sold | SUM | Category, Time | ✓ |
| KPI-012 | Top Products | RANK | SKU | - |
| KPI-013 | Category Mix | RATIO | Category | ✓ |
| KPI-014 | Return Rate | RATIO | Category, Time | ✓ |
| KPI-015 | Discount Utilization | RATIO | Time | ✓ |
| KPI-016 | Avg Discount % | AVG | Segment | ✓ |
| KPI-017 | Gift Order Rate | RATIO | Time | ✓ |
| KPI-018 | Carbon per Order | RATIO | Time | ✓ |
| KPI-019 | Sustainable Packaging | RATIO | Time | ✓ |
| KPI-020 | Energy Intensity | RATIO | Time | ✓ |

---

## References

- [KPI Definitions Config](../configs/kpi_definitions.yaml)
- [Methodology](./methodology.md)
- [Architecture](./architecture.md)
