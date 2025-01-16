SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [mdl].[Customer_Loyalty_Store_Reporting] AS With

AX_Loyalty_Discount as
(
SELECT [TransactionID]
      ,[Store]
      ,[Transdate]
      ,[LOYALTYCARDID]
      ,cast(Recid as nvarchar) Recid
      ,[Item]
      ,[Discount_Percentage]
      ,[PERIODICDISCOUNTOFFERID]
      ,[PromotionCode]
      ,sum([Discount_Amount]) Discount_Amount
  FROM (    Select *
            from [stg].[LoyaltyCard_Transaction_Discount_v2]
            where Discount_Amount is not null
            and store not like 'GB01%'
        ) as a
  where Discount_Amount is not null
 group by [TransactionID],[Store], [Transdate], [LOYALTYCARDID], Recid
 ,[Item], [Discount_Percentage],[PERIODICDISCOUNTOFFERID], [PromotionCode]
),

Mishi_Loyalty_Discount AS
(
SELECT [TransactionID]
      ,[Store]
      ,[Transdate]
      ,[LOYALTYCARDID]
      ,cast(Recid as nvarchar) Recid
      ,[Item]
      ,[Discount_Percentage]
      ,[PERIODICDISCOUNTOFFERID]
      ,[PromotionCode]
      ,sum([Discount_Amount]) Discount_Amount
  FROM (    Select *
            from [stg].[LoyaltyCard_Transaction_Discount_GB]
            where Discount_Amount is not null
        ) as a
  where Discount_Amount is not null
 group by [TransactionID],[Store], [Transdate], [LOYALTYCARDID], Recid
 ,[Item], [Discount_Percentage],[PERIODICDISCOUNTOFFERID], [PromotionCode]
),

Loyalty_Discount as
(
Select * from AX_Loyalty_Discount
UNION ALL
Select * from Mishi_Loyalty_Discount
),

base_cl as
(
Select s.[Date], s.[LOYALTYCARDID], [Currency Code], s.[Item], s.[Entity], [Sales Header Pos ID], 
s.[Store Number] StoreNumber,
ld.[Discount_Percentage],
-- Hard fix to capture Black Friday issue for the 3 dates below for GB to set discount to 40% instead of 10%
Case when s.date between '2024-11-29' and '2024-12-01' and s.Entity = 'GB01' and [PromotionCode] ='Club Voucher 10%' then 40 else [Discount_Percentage] end as [Discount Percentage],
 
ld.[PromotionCode],
ld.PERIODICDISCOUNTOFFERID,
Case when s.[LOYALTYCARDID] is not null and s.[LOYALTYCARDID] != '' then [Sales Header Pos ID] end as LOYALTYCARD_Transaction,
Case when s.[LOYALTYCARDID] is not null and s.[LOYALTYCARDID] != '' then s.[LOYALTYCARDID] else null end as LOYALTYCARD_Customers,

Case when s.[LOYALTYCARDID] is not null and s.[LOYALTYCARDID] != '' then [Sales Lcy] else 0 end as LOYALTYCARD_SalesLCY,
Case when s.[LOYALTYCARDID] is not null and s.[LOYALTYCARDID] != '' then ([Sales Lcy] + ISNULL([Amount Tax], 0)) else 0 end as LOYALTYCARD_SalesLCY_inc_tax,

Case when s.[LOYALTYCARDID] is not null and s.[LOYALTYCARDID] != '' then [Sales Lcy] * ex.[ExchangeRate] else 0 end as LOYALTYCARD_SalesDKK,
Case when s.[LOYALTYCARDID] is not null and s.[LOYALTYCARDID] != '' then ([Sales Lcy] + ISNULL([Amount Tax], 0)) * ex.[ExchangeRate] else 0 end as LOYALTYCARD_SalesDKK_inc_Tax,

Case when s.[LOYALTYCARDID] is not null and s.[LOYALTYCARDID] != '' then [Amount Discount] *  ex.[ExchangeRate] * -1 else 0 end as LOYALTYCARD_DiscountDKK,

Case when s.[LOYALTYCARDID] is not null and s.[LOYALTYCARDID] != '' then ([Sales Lcy] + ISNULL([Amount Discount] * -1, 0)) *  ex.[ExchangeRate] else 0 end as LOYALTYCARD_SalesPriceDKK,
Case when s.[LOYALTYCARDID] is not null and s.[LOYALTYCARDID] != '' then [Quantity] else 0 end as LOYALTYCARD_Quantity,

-- ROW_NUMBER() over (Partition by [Sales Header Pos ID] order by item) transaction_line_number,
[Quantity], 
[Sales Lcy] * ex.[ExchangeRate] SalesDKK, 
([Sales Lcy] + ISNULL([Amount Tax], 0)) * ex.[ExchangeRate] SalesDKK_Inc_Tax,
[Amount Discount] *  ex.[ExchangeRate] * -1 AmountDiscountDKK,
([Sales Lcy] + ISNULL([Amount Discount]* -1, 0)) *  ex.[ExchangeRate] Sales_Price_DKK,
Case when row_number() over (partition by [Sales Header Pos ID] order by s.Item) = 1 then
    Count(s.Item) over (partition by [Sales Header Pos ID] ) else 0 end as Transaction_Item_count,
Case when row_number() over (partition by [Sales Header Pos ID] order by s.Item) = 1 and s.[LOYALTYCARDID] is not null and s.[LOYALTYCARDID] != '' then
    Count(s.Item) over (partition by [Sales Header Pos ID] ) else 0 end as Loyalty_Transaction_Item_count
    
from [mdl].[_mst_SalesLinePos] s
left join [mdl].[_mst_ExchangeRate] ex 
	on ex.[CurrencyCode] = s.[Currency Code]
left join Loyalty_Discount as ld
    on s.[Sales Header Pos ID] = ld.Recid and s.[Store Number] = ld.[Store]
    and s.[Item] = ld.[Item]
where [Flag Canceled] <> '1' and [Flag Line Is Returned] <> '1'  and [Quantity] > 0
-- Add new Entities here and data of launch
and (  (s.[Date] > '2024-06-25' and entity ='DK01') or (s.[Date] > '2024-09-23' and entity ='NO01') or (s.[Date] > '2024-09-23' and entity ='SE01') 
or (s.[Date] > '2024-09-30' and entity ='FI01') or (s.[Date] > '2024-10-07' and entity ='DE01') or (s.[Date] > '2024-10-28' and entity ='GB01') 
)
)


Select Date, StoreNumber, Entity,
Case when [LOYALTYCARDID] is not null and [LOYALTYCARDID] != '' then LOYALTYCARDID end as LOYALTYCARDID ,
[Sales Header Pos ID] Order_ID,
[LOYALTYCARD_Transaction] Loyalty_Order_ID,
Discount_Percentage,[Discount Percentage], [PromotionCode], [PERIODICDISCOUNTOFFERID],

Count(Distinct [Sales Header Pos ID]) Transactions,
Count(distinct LOYALTYCARD_Transaction) Loyalty_Transactions,
SUM(SalesDKK) SalesDKK,
SUM(SalesDKK_Inc_Tax) SalesDKK_Inc_Tax,
SUM(AmountDiscountDKK) AmountDiscountDKK,
SUM(Sales_Price_DKK) Sales_Price_DKK,
SUM(Transaction_Item_count) Transaction_Item_count,
SUM([Quantity]) Quantity, --Quantity_Items
SUM(LOYALTYCARD_SalesLCY) LOYALTYCARD_SalesLCY,
SUM(LOYALTYCARD_SalesLCY_inc_tax) LOYALTYCARD_SalesLCY_inc_tax,
SUM(LOYALTYCARD_SalesDKK) LOYALTYCARD_SalesDKK,
SUM(LOYALTYCARD_SalesDKK_inc_Tax) LOYALTYCARD_SalesDKK_inc_Tax,
SUM(LOYALTYCARD_DiscountDKK) LOYALTYCARD_DiscountDKK,
SUM(LOYALTYCARD_SalesPriceDKK) LOYALTYCARD_SalesPriceDKK,
SUM(Loyalty_Transaction_Item_count) Loyalty_Transaction_Item_count,
SUM(LOYALTYCARD_Quantity) LOYALTYCARD_Quantity

from base_cl
group by Date, StoreNumber, Entity, LOYALTYCARDID, [Sales Header Pos ID], LOYALTYCARD_Transaction, Discount_Percentage, [Discount Percentage],
[PromotionCode], [PERIODICDISCOUNTOFFERID];
GO
