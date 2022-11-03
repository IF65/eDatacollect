SELECT 'F'                 tipo,
       case
           when left(sh.code, 4) = '0600' then '0502'
           when left(sh.code, 4) = '0700' then '0503'
           else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2) + '0' +
       right(ts.code, 2)   id,
       ''                  codice,
       count(*)            s1,
       sum(t.total_amount) s2
FROM TCPOS4.dbo.transactions t
         join TCPOS4.dbo.tills ts on t.till_id = ts.id
         join TCPOS4.dbo.shops sh on t.shop_id = sh.id
where convert(DATE, t.trans_date) = '2022-11-02'
  and ts.code >= '021'
  and ts.code <= '029'
  and t.delete_timestamp is null
group by convert(DATE, t.trans_date),
         case
             when left(sh.code, 4) = '0600' then '0502'
             when left(sh.code, 4) = '0700' then '0503'
             else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2) + '0' +
         right(ts.code, 2)
UNION
SELECT 'H'                                                                     tipo,
       case
           when left(sh.code, 4) = '0600' then '0502'
           when left(sh.code, 4) = '0700' then '0503'
           else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) id,
       replace(CONVERT(VARCHAR (8), max(t.trans_date), 108), ':', '')          codice,
       0                                                                       s1,
       0                                                                       s2
FROM TCPOS4.dbo.transactions t
         join TCPOS4.dbo.tills ts on t.till_id = ts.id
         join TCPOS4.dbo.shops sh on t.shop_id = sh.id
where convert(DATE, t.trans_date) = '2022-11-02'
  and ts.code >= '021'
  and ts.code <= '029'
  and t.delete_timestamp is null
group by case
             when left(sh.code, 4) = '0600' then '0502'
             when left(sh.code, 4) = '0700' then '0503'
             else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112)
union
SELECT 'T'               tipo,
       case
           when left(sh.code, 4) = '0600' then '0502'
           when left(sh.code, 4) = '0700' then '0503'
           else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2) + '0' +
       right(ts.code, 2) id,
       case
           when pay.payment_id = 1 then '01'
           when pay.payment_id = 5 then '04'
           when pay.payment_id = 10 then '08'
           when pay.payment_id = 11 then '05'
           when pay.payment_id = 12 then '08'
           when pay.payment_id = 13 then '01'
           else
               '00'
           end           codice,
       sum(pay.amount)   s1,
       0                 s2
FROM TCPOS4.dbo.transactions t
         join TCPOS4.dbo.tills ts on t.till_id = ts.id
         join TCPOS4.dbo.shops sh on t.shop_id = sh.id
         join TCPOS4.dbo.trans_payments pay on t.id = pay.transaction_id
where pay.credit_card_id is null
  and pay.voucher_id is null
  and convert(DATE, t.trans_date) = '2022-11-02'
  and ts.code >= '021'
  and ts.code <= '029'
  and t.delete_timestamp is null
group by case
             when left(sh.code, 4) = '0600' then '0502'
             when left(sh.code, 4) = '0700' then '0503'
             else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2) + '0' +
         right(ts.code, 2),
         case
             when pay.payment_id = 1 then '01'
             when pay.payment_id = 5 then '04'
             when pay.payment_id = 10 then '08'
             when pay.payment_id = 11 then '05'
             when pay.payment_id = 12 then '08'
             when pay.payment_id = 13 then '01'
             else
                 '00'
             end
union
select 'T'                  tipo,
       case
           when left(sh.code, 4) = '0600' then '0502'
           when left(sh.code, 4) = '0700' then '0503'
           else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2) + '0' +
       right(ts.code, 2)    id,
       cc.card_number_ident codice,
       sum(tp.amount)       s1,
       0                    s2
from TCPOS4.dbo.transactions t
         join TCPOS4.dbo.tills ts on t.till_id = ts.id
         join TCPOS4.dbo.shops sh on t.shop_id = sh.id
         join TCPOS4.dbo.trans_payments tp on tp.transaction_id = t.id
         join TCPOS4.dbo.credit_cards cc on tp.credit_card_id = cc.id
where convert(DATE, t.trans_date) = '2022-11-02'
  and ts.code >= '021'
  and ts.code <= '029'
  and t.delete_timestamp is null
group by case
             when left(sh.code, 4) = '0600' then '0502'
             when left(sh.code, 4) = '0700' then '0503'
             else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2) + '0' +
         right(ts.code, 2), cc.card_number_ident
union
SELECT 'T'               tipo,
       case
           when left(sh.code, 4) = '0600' then '0502'
           when left(sh.code, 4) = '0700' then '0503'
           else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2) + '0' +
       right(ts.code, 2) id,
       4                 codice,
       sum(pay.amount)   s1,
       0                 s2
FROM TCPOS4.dbo.transactions t
         join TCPOS4.dbo.tills ts on t.till_id = ts.id
         join TCPOS4.dbo.shops sh on t.shop_id = sh.id
         join TCPOS4.dbo.trans_payments pay on t.id = pay.transaction_id
where pay.voucher_id is not null
  and convert(DATE, t.trans_date) = '2022-11-02'
  and ts.code >= '021'
  and ts.code <= '029'
  and t.delete_timestamp is null
group by case
             when left(sh.code, 4) = '0600' then '0502'
             when left(sh.code, 4) = '0700' then '0503'
             else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2) + '0' +
         right(ts.code, 2)
union
select 'V'                             tipo,
       case
           when left(sh.code, 4) = '0600' then '0502'
           when left(sh.code, 4) = '0700' then '0503'
           else left(sh.code, 4) end + convert(varchar (8),
                                               t.trans_date, 112) + '0' + right(ts.code, 2) + '0' +
       right(ts.code, 2)               id,
       isnull(case when tv.vat_id = 1 then 2 when tv.vat_id = 4 then 1 else tv.vat_id end,
              2)                       codice,
       sum(case
               when tv.vat_id is null then round(tv.gross_amount * 10 / 100, 2)
               else tv.vat_amount end) s1,
       sum(case
               when tv.vat_id is null then round(tv.gross_amount - round(tv.gross_amount * 10 / 100, 2), 2)
               else tv.net_amount end) s2
FROM TCPOS4.dbo.transactions t
         join TCPOS4.dbo.tills ts on t.till_id = ts.id
         join TCPOS4.dbo.shops sh on t.shop_id = sh.id
         join TCPOS4.dbo.trans_vats tv on t.id = tv.transaction_id
where convert(DATE, t.trans_date) = '2022-11-02'
  and ts.code >= '021'
  and ts.code <= '029'
  and t.delete_timestamp is null
group by case
             when left(sh.code, 4) = '0600' then '0502'
             when left(sh.code, 4) = '0700' then '0503'
             else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2) + '0' +
         right(ts.code, 2), isnull(case when tv.vat_id = 1 then 2 when tv.vat_id = 4 then 1 else tv.vat_id end, 2)
UNION
SELECT 'C'                                                                                                tipo,
       case
           when left(s.code, 4) = '0600' then '0502'
           when left(s.code, 4) = '0700' then '0503'
           else left(s.code, 4) end + convert(varchar (8), c.opening_date, 112) + '0' + right(ts.code, 2) id,
       0                                                                                                  codice,
       sum(dc.drawer_count_amount)                                                                        s1,
       0                                                                                                  s2
FROM TCPOS4.dbo.shops s
         join TCPOS4.dbo.tills ts on s.id = ts.shop_id
         join TCPOS4.dbo.closings c on c.till_id = ts.id
         join TCPOS4.dbo.drawer_counts dc on c.id = dc.closing_id
where convert(DATE, c.opening_date) = '2022-11-02'
  and ts.code >= '021'
  and ts.code <= '029'
group BY case
             when left(s.code, 4) = '0600' then '0502'
             when left(s.code, 4) = '0700' then '0503'
             else left(s.code, 4) end + convert(varchar (8), c.opening_date, 112) + '0' + right(ts.code, 2)
union
SELECT 'I'                                                                                               tipo,
       case
           when left(sh.code, 4) = '0600' then '0502'
           when left(sh.code, 4) = '0700' then '0503'
           else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2) id,
       ts.code                                                                                           codice,
       count(*)                                                                                          s1,
       sum(t.total_amount)                                                                               s2
FROM TCPOS4.dbo.transactions t
         join TCPOS4.dbo.tills ts on t.till_id = ts.id
         join TCPOS4.dbo.shops sh on t.shop_id = sh.id
where convert(DATE, t.trans_date) = '2022-11-02'
  and ts.code >= '021'
  and ts.code <= '029'
  and t.delete_timestamp is null
group by case
             when left(sh.code, 4) = '0600' then '0502'
             when left(sh.code, 4) = '0700' then '0503'
             else left(sh.code, 4) end + convert(varchar (8), t.trans_date, 112) + '0' + right(ts.code, 2), ts.code
UNION
SELECT 'R'                                               tipo,
       case
           when left(sh.code, 4) = '0600' then '0502'
           when left(sh.code, 4) = '0700' then '0503'
           else
               left(sh.code, 4)
           end + convert(varchar (8), t.trans_date, 112) id,
       case
           when a.group_c_id = 203 then 8
           when a.group_c_id = 76 then 5
           when a.group_c_id = 150 then 6
           when a.group_c_id = 149 then 7
           else
               1
           end                                           codice,
       sum(ta.price + coalesce(ta.discount, 0))          s1,
       0                                                 s2
FROM TCPOS4.dbo.transactions t
         join TCPOS4.dbo.tills ts on t.till_id = ts.id
         join TCPOS4.dbo.shops sh on t.shop_id = sh.id
         join TCPOS4.dbo.trans_articles ta on t.id = ta.transaction_id
         join TCPOS4.dbo.articles a on a.id = ta.article_id
where ta.delete_timestamp is NULL
  and convert(DATE, t.trans_date) = '2022-11-02'
  and ts.code >= '021'
  and ts.code <= '029'
  and t.delete_timestamp is null
group by case
             when left(sh.code, 4) = '0600' then '0502'
             when left(sh.code, 4) = '0700' then '0503'
             else
                 left(sh.code, 4)
             end + convert(varchar (8), t.trans_date, 112),
         case
             when a.group_c_id = 203 then 8
             when a.group_c_id = 76 then 5
             when a.group_c_id = 150 then 6
             when a.group_c_id = 149 then 7
             else
                 1
             end,
         case
             when a.group_c_id = 203 then 8
             when a.group_c_id = 76 then 5
             when a.group_c_id = 150 then 6
             when a.group_c_id = 149 then 7
             else
                 1
             end;
