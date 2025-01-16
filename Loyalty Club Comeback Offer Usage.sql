SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [mdl].[Customer_Loyalty_Reporting_ComeBack] AS WITH 
cte_reward AS (
    SELECT [Customer], [Date], [properties.reward] AS reward_code, [properties.coupon] AS coupon
    FROM [stg].[antavo_loyalty]
    WHERE [Action] = 'Reward' 
)

, cte_redeemed AS (
    SELECT [Customer], [Date] Redeem_date, [timestamp] redeem_timestamp,[properties.code] AS Code
    FROM [stg].[antavo_loyalty]
    WHERE[Action] = 'coupon_redeem' 
)

, cte_unredeemed AS (
    SELECT [Customer], [Date], [properties.code] AS Code
    FROM [stg].[antavo_loyalty]
    WHERE[Action] = 'coupon_unredeem' 
)

, cte_redeemed_unredeemed AS
(
SELECT r.*, u.Code Unredeemed_code,
Case when U.Customer is Null then 1 else 0 end as Redeemed_not_unredeemed
from cte_redeemed r
left join cte_unredeemed u
on r.Customer = u.Customer and r.Code = u.Code and r.Redeem_date = u.date
)


, cte_rewards_lookup AS (
    SELECT *
    FROM [stg].[antavo_list_rewards]
    WHERE cf_discount_name = 'Club Voucher 25%' 
)

, cte_eligible AS (
    SELECT r.*, l.name, l.cf_discount_code, 'Y' AS Eligible, starts_at, ends_at
    FROM cte_reward r
    INNER JOIN cte_rewards_lookup l
    ON r.reward_code = l.ID
)

, cte_eligible_redeemed as
(
Select e.*, ru.Redeemed_not_unredeemed, ru.Redeem_date, ru.redeem_timestamp
from cte_eligible e
left join cte_redeemed_unredeemed ru
on e.customer = ru.customer and e.coupon = ru.Code and Redeemed_not_unredeemed = 1
)


, cte_checkout AS (
    SELECT 
        a.[Customer], a.[Action], a.[Date], a.[Timestamp] Transaction_timestamp, 
        [properties.transaction_id] AS Transaction_ID, 
        [properties.discount_code] AS Discount_Code,
        [properties.code] AS coupon_code,
        SUM([properties.subtotal]) AS Total
    FROM [stg].[antavo_loyalty] a
    inner join cte_eligible_redeemed r
    on a.Customer = r.Customer and a.[properties.discount_code] = r.cf_discount_code and Redeemed_not_unredeemed = 1 and a.date = r.redeem_date
    WHERE [Action] = 'Checkout_item' 
    GROUP BY a.[Customer], a.[Action], a.[Date], a.[Timestamp], [properties.transaction_id], [properties.discount_code], [properties.code]
)

Select 
Customer, Date Reward_date, Reward_code, Coupon, Name,
cf_Discount_code as discount_code, Eligible, Starts_at, Ends_at, Redeemed_not_unredeemed, Redeem_Date, redeem_timestamp,
-- Logic built in to deal with cases where more than 1 Comeback voucher was redeemed on the same day and cannot be distinctly assigned a transaction so take the first case only.
Transaction_timestamp,
Case when Transaction_ID = Lag_transaction_ID then null else Purchase_date end as Purchase_Date,
Case when Transaction_ID = Lag_transaction_ID then null else Transaction_ID end as Transaction_ID,
Case when Transaction_ID = Lag_transaction_ID then null else Total end as Total
from
(
SELECT e.*, 
       p.Date AS Purchase_date, 
       p.Transaction_timestamp,
       p.Transaction_ID, 
       LAG(Transaction_ID) over (partition by e.Customer, coupon order by e.redeem_timestamp) Lag_transaction_ID,
       p.Total
FROM cte_eligible_redeemed e
LEFT JOIN cte_checkout p
    ON e.Customer = p.Customer 
    AND e.cf_discount_code = p.discount_code
    AND p.Date = e.redeem_date
    AND p.Transaction_timestamp > e.redeem_timestamp
    AND e.redeem_timestamp between redeem_timestamp and Dateadd(mi, 30, redeem_timestamp) -- ensure voucher is used in 30 mins
    AND Redeemed_not_unredeemed = 1
) as A
Where Lag_transaction_ID is null;
GO
