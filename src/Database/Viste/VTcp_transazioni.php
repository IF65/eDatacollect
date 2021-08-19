<?php

namespace Database\Viste;

use PDOException;

class VTcp_transazioni
{

    private $hostname = '10.11.14.250';
    private $dbname = 'TCPOS4';
    private $username = 'sa';
    private $password = 'vfr456YHN';

    //private $mssqldriver = '{ODBC Driver 13 for SQL Server}';

    public function __construct()
    {
        //parent::__construct();
    }

    public function recuperaDatiPerQuadratura(array $request): string {
        $data = $request['data'];

        if ($request['sede'] == '0501') {
            $tillSearch = " and ts.code >= '021' and ts.code <= '029'";
        } elseif ($request['sede'] == '0502') {
            $tillSearch = " and ts.code >= '031' and ts.code <= '039'";
        } elseif ($request['sede'] == '0503') {
            $tillSearch = " and ts.code >= '041' and ts.code <= '049'";
        } elseif ($request['sede'] == '0201') {
	        $tillSearch = " and ts.code >= '021' and ts.code <= '029'";
        } elseif ($request['sede'] == '0155') {
	        $tillSearch = " and ts.code >= '021' and ts.code <= '029'";
        } else {
            $tillSearch = " and ts.code >= '021' and ts.code <= '029'";
        }

        $conn = new \PDO("sqlsrv:Server=".$this->hostname.",9089;Database=".$this->dbname, $this->username, $this->password);

        $conn->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
        $conn->setAttribute( \PDO::SQLSRV_ATTR_QUERY_TIMEOUT, 10 );

        $stmt = "   SELECT 'F' tipo, case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2) + '0' + right(ts.code,2) id, '' codice, count(*) s1, sum(t.total_amount) s2
                    FROM TCPOS4.dbo.transactions t 
                        join TCPOS4.dbo.tills ts on t.till_id = ts.id 
                        join TCPOS4.dbo.shops sh on t.shop_id = sh.id 
                    where convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null
                    group by convert(DATE, t.trans_date), case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2) + '0' + right(ts.code,2)
                    UNION 
                    SELECT 'H' tipo, case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) id, replace(CONVERT(VARCHAR(8),max(t.trans_date),108),':','')  codice, 0 s1, 0 s2
                    FROM TCPOS4.dbo.transactions t 
                        join TCPOS4.dbo.tills ts on t.till_id = ts.id 
                        join TCPOS4.dbo.shops sh on t.shop_id = sh.id 
                    where convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null
                    group by case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112)
                    union
                    SELECT 'T' tipo, case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2) + '0' + right(ts.code,2) id, 
                        case 
                            when pay.payment_id = 1 then '01'
                            when pay.payment_id = 5 then '04'
                            when pay.payment_id = 10 then '08'
                            when pay.payment_id = 11 then '05'
                            when pay.payment_id = 12 then '08'
                            when pay.payment_id = 13 then '01'
                        else 
                            '00'
                        end codice, sum(pay.amount) s1, 0 s2
                    FROM TCPOS4.dbo.transactions t 
                        join TCPOS4.dbo.tills ts on t.till_id = ts.id 
                        join TCPOS4.dbo.shops sh on t.shop_id = sh.id
                        join TCPOS4.dbo.trans_payments pay on t.id = pay.transaction_id 
                    where pay.credit_card_id is null and pay.voucher_id is null and convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null
                    group by case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2) + '0' + right(ts.code,2), 
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
                select 'T' tipo, case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2) + '0' + right(ts.code,2) id, cc.card_number_ident codice,sum(tp.amount ) s1, 0 s2
                from TCPOS4.dbo.transactions t
                    join TCPOS4.dbo.tills ts on t.till_id = ts.id 
                    join TCPOS4.dbo.shops sh on t.shop_id = sh.id
                    join TCPOS4.dbo.trans_payments tp on tp.transaction_id =t.id 
                    join TCPOS4.dbo.credit_cards cc on tp.credit_card_id =cc.id 
                where convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null
                group by case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2) + '0' + right(ts.code,2), cc.card_number_ident
                union
                SELECT 'T' tipo, case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2) + '0' + right(ts.code,2) id, 4 codice, sum(pay.amount) s1, 0 s2
                FROM TCPOS4.dbo.transactions t 
                    join TCPOS4.dbo.tills ts on t.till_id = ts.id 
                    join TCPOS4.dbo.shops sh on t.shop_id = sh.id
                    join TCPOS4.dbo.trans_payments pay on t.id = pay.transaction_id 
                where pay.voucher_id is not null and convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null
                group by case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2) + '0' + right(ts.code,2)
                union
                select 'V' tipo, 
                    case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), 
                    t.trans_date, 112) + '0' + right(ts.code,2) + '0' + right(ts.code,2) id, 
                    isnull(case when tv.vat_id = 1 then 2 when  tv.vat_id = 4 then 1 else tv.vat_id end, 2) codice, 
                    sum(case when tv.vat_id is null then round(tv.gross_amount * 10 /100,2) else tv.vat_amount end) s1, 
                    sum(case when tv.vat_id is null then round(tv.gross_amount - round(tv.gross_amount * 10 /100,2),2) else tv.net_amount end) s2
                FROM TCPOS4.dbo.transactions t 
                    join TCPOS4.dbo.tills ts on t.till_id = ts.id 
                    join TCPOS4.dbo.shops sh on t.shop_id = sh.id
                    join TCPOS4.dbo.trans_vats tv on t.id = tv.transaction_id 
                where convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null
                group by case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2) + '0' + right(ts.code,2), isnull(case when tv.vat_id = 1 then 2 when  tv.vat_id = 4 then 1 else tv.vat_id end, 2)
                UNION
                SELECT 
                    'C' tipo,
                    case when left(s.code, 4) = '0600' then '0502' when left(s.code, 4) = '0700' then '0503' else left(s.code, 4) end + convert(varchar(8), c.opening_date, 112) + '0' + right(ts.code,2)  id, 
                    0 codice, 
                    sum(dc.drawer_count_amount ) s1, 
                    0 s2
                FROM TCPOS4.dbo.shops s 
                    join TCPOS4.dbo.tills ts on s.id =ts.shop_id
                    join TCPOS4.dbo.closings c on c.till_id = ts.id
                    join TCPOS4.dbo.drawer_counts dc on c.id =dc.closing_id 
                where convert(DATE, c.opening_date ) = '$data' $tillSearch 
                group BY case when left(s.code, 4) = '0600' then '0502' when left(s.code, 4) = '0700' then '0503' else left(s.code, 4) end + convert(varchar(8), c.opening_date, 112) + '0' + right(ts.code,2) 
                union 
                SELECT 'I' tipo, case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2) id, ts.code codice, count(*) s1, sum(t.total_amount) s2
                FROM TCPOS4.dbo.transactions t 
                    join TCPOS4.dbo.tills ts on t.till_id = ts.id 
                    join TCPOS4.dbo.shops sh on t.shop_id = sh.id
                where convert(DATE, t.trans_date ) = '$data' $tillSearch and t.delete_timestamp is null
                group by case when left(sh.code, 4) = '0600' then '0502' when left(sh.code, 4) = '0700' then '0503' else left(sh.code, 4) end + convert(varchar(8), t.trans_date, 112) + '0' + right(ts.code,2), ts.code
                UNION
                SELECT 
                    'R' tipo, 
                    case 
                        when left(sh.code, 4) = '0600' then '0502' 
                        when left(sh.code, 4) = '0700' then '0503' 
                    else 
                        left(sh.code, 4)
                    end + convert(varchar(8), t.trans_date, 112) id, 
                    case 
                        when a.group_c_id = 203 then 8 
                        when a.group_c_id = 76 then 5 
                        when a.group_c_id = 150 then 6 
                        when a.group_c_id = 149 then 7 
                    else
                        1 
                    end codice, 
                    sum(ta.price + coalesce(ta.discount, 0)) s1, 
                    0 s2
                FROM TCPOS4.dbo.transactions t 
                    join TCPOS4.dbo.tills ts on t.till_id = ts.id 
                    join TCPOS4.dbo.shops sh on t.shop_id = sh.id
                    join TCPOS4.dbo.trans_articles ta on t.id = ta.transaction_id 
                    join TCPOS4.dbo.articles a on a.id = ta.article_id 
                where ta.delete_timestamp is NULL and convert(DATE, t.trans_date ) = '$data' $tillSearch and t.delete_timestamp is null
                group by 
	                case 
                        when left(sh.code, 4) = '0600' then '0502' 
                        when left(sh.code, 4) = '0700' then '0503' 
                    else 
                        left(sh.code, 4) 
                    end + convert(varchar(8), t.trans_date, 112), 
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
                ";

        $result = [];

        $stmt = $conn->query( $stmt );
        while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC ) ){
            $result[] = $row;
        }
        return json_encode($result, true);
    }

    public function creazioneDatacollectTcPos(array $request): string {
        $data = $request['data'];

        if ($request['sede'] == '0501') {
            $tillSearch = " and ts.code >= '021' and ts.code <= '029' and sh.code = '0500' ";
        } elseif ($request['sede'] == '0502') {
            $tillSearch = " and ts.code >= '031' and ts.code <= '039' and sh.code = '0600' ";
        } elseif ($request['sede'] == '0503') {
            $tillSearch = " and ts.code >= '041' and ts.code <= '049' and sh.code = '0700' ";
        } else {
            $tillSearch = " and sh.code = '" . $request['sede'] . "' ";
        }

        $conn = new \PDO("sqlsrv:Server=".$this->hostname.",9089;Database=".$this->dbname, $this->username, $this->password);

        $conn->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
        $conn->setAttribute( \PDO::SQLSRV_ATTR_QUERY_TIMEOUT, 10 );

        $stmt = "	select t.id trans_id, convert(varchar, t.trans_date, 126) trans_date, t.total_amount, t.trans_num, ts.code till_code, o.code operator_code, t.card_num
					FROM TCPOS4.dbo.transactions t 
						join TCPOS4.dbo.tills ts on t.till_id = ts.id 
						join TCPOS4.dbo.operators o  on t.operator_id = o.id 
						join TCPOS4.dbo.shops sh on t.shop_id = sh.id 
					where convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null";

        $transactions = [];
        $stmt = $conn->query( $stmt );
        while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC ) ){
            $trans_id = $row['trans_id'];
            unset($row['trans_id']);
            $transactions[$trans_id] = $row;
            $transactions[$trans_id]['articles'] = [];
            $transactions[$trans_id]['points'] = [];
	        $transactions[$trans_id]['discounts'] = [];
	        $transactions[$trans_id]['promotions'] = [];
        }

        $stmt = "	select 
						ta.transaction_id trans_id,
						ta.hash_code,
						coalesce(ta.owner_hash_code,0) owner_hash_code,
						coalesce(ta.menu_id, 0) menu_id,
						a.code article_code,
					    a.description, 
						coalesce(ta.vat_percent, 0) vat_percent,
						case when ta.vat_id = 1 then 7 when ta.vat_id = 4 then 1 else ta.vat_id end vat_code,
						coalesce(ta.qty_weight,0) qty_weight,
						coalesce(ta.price,0) price,
						coalesce(ta.discount,0) discount,
						coalesce(ta.promotion_discount,0) promotion_discount,
						coalesce(ta.pricelevel_unit_price, ta.price) pricelevel_unit_price,
						coalesce(ta.price_overridden, 0) price_overridden,		
	       				0 addition_article_price 
					from transactions t 
						join trans_articles ta on t.id = ta.transaction_id 
						join articles a on a.id = ta.article_id
						join TCPOS4.dbo.tills ts on t.till_id = ts.id 
						join TCPOS4.dbo.shops sh on t.shop_id =sh.id 
					where convert(DATE, t.trans_date) = '$data' $tillSearch and ta.addition_article_hash_code is null and 
					      ta.addition_menu_hash_code is null and t.delete_timestamp is null and ta.delete_timestamp is null and 
					      ta.delete_operator_id is null;";

        $stmt = $conn->query( $stmt );
        while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC ) ) {
            $trans_id = $row['trans_id'];
            if (key_exists($trans_id, $transactions)) {
                $articles = [];
                if (key_exists('articles', $transactions[$trans_id])) {
                    $articles = $transactions[$trans_id]['articles'];
                }
                unset($row['trans_id']);

                $articles[$row['hash_code']] = $row;

                $transactions[$trans_id]['articles'] = $articles;
            }
        }

        // aggancio gli addition
        $stmt = "select 
					ta.transaction_id trans_id,
					ta.price, 
					ta.addition_article_hash_code,
					ta.addition_menu_hash_code 
				from transactions t 
					join trans_articles ta on t.id = ta.transaction_id 
					join TCPOS4.dbo.tills ts on t.till_id = ts.id 
					join TCPOS4.dbo.shops sh on t.shop_id =sh.id 
				where convert(DATE, t.trans_date) = '$data' $tillSearch and  (ta.addition_article_hash_code is not null or ta.addition_menu_hash_code is not null) and 
				      t.delete_timestamp is null and ta.delete_timestamp is null and ta.delete_operator_id is null;";
	    $stmt = $conn->query( $stmt );
	    while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC ) ) {
		    $trans_id = $row['trans_id'];
		    if (key_exists($trans_id, $transactions)) {
		    	if (key_exists($row['addition_article_hash_code'], $transactions[$trans_id]['articles'])) {
				    $transactions[$trans_id]['articles'][$row['addition_article_hash_code']]['addition_article_price'] =
					    round($transactions[$trans_id]['articles'][$row['addition_article_hash_code']]['addition_article_price'] + $row['price'],2);
			    }
		    }
	    }

	    //calcolo i menu
	    foreach($transactions as $trans_id => $transaction) {
		    $menus = [];
		    foreach($transaction['articles'] as $hash_code => $article) {
				$owner_hash_code = $article['owner_hash_code'];
				if ($owner_hash_code != '0') {
					if (!key_exists($owner_hash_code, $menus)) {
						$menus[$owner_hash_code] = ['id' => $article['menu_id'], 'price' => 0, 'articles' => []];
					}
					$menus[$owner_hash_code]['price'] += round(round($article['pricelevel_unit_price'] * $article['qty_weight'], 2) - $article['price'],2);
					$menus[$owner_hash_code]['articles'][] = $hash_code;
				}
		    }
		    $transactions[$trans_id]['menus'] = $menus;
	    }

	    //calcolo gli sconti
	    $stmt = "	select t.id trans_id, td.discount_id, sum(td.amount) amount 
					FROM TCPOS4.dbo.transactions t 
						join TCPOS4.dbo.tills ts on t.till_id = ts.id 
						join TCPOS4.dbo.trans_discounts td on t.id = td.transaction_id 
						join TCPOS4.dbo.shops sh on t.shop_id =sh.id 
					where convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null 
					group by t.id, td.discount_id;";
	    $stmt = $conn->query( $stmt );
	    while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC ) ) {
		    $trans_id = $row['trans_id'];
		    unset($row['trans_id']);
		    $transactions[$trans_id]['discounts'][] = $row;
	    }

	    //calcolo le promozioni
	    $stmt = "	select t.id trans_id, tp.promotion_id, sum(tp.discount + tp.amount + tp.offered_amount) amount 
					FROM TCPOS4.dbo.transactions t 
						join TCPOS4.dbo.tills ts on t.till_id = ts.id 
						join TCPOS4.dbo.trans_promotions tp on tp.transaction_id = t.id 
						join TCPOS4.dbo.shops sh on t.shop_id =sh.id 
					where convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null
					group by t.id, tp.promotion_id;";
	    $stmt = $conn->query( $stmt );
	    while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC ) ) {
		    $trans_id = $row['trans_id'];
		    unset($row['trans_id']);
		    if ($row['amount'] <> 0) {
			    $transactions[$trans_id]['promotions'][] = $row;
		    }
	    }

		//calcolo i punti
        $stmt = "	select t.id trans_id, tpc.points_balance, tpc.points_gained, tpc.points_spent, tpc.points_used, pc.code, pc.description 
					FROM TCPOS4.dbo.transactions t 
						join TCPOS4.dbo.tills ts on t.till_id = ts.id 
						join TCPOS4.dbo.trans_point_campaigns tpc on t.id = tpc.transaction_id 
						join TCPOS4.dbo.point_campaigns pc on tpc.point_campaign_id =pc.id 
						join TCPOS4.dbo.shops sh on t.shop_id =sh.id 
					where convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null;
					";
        $stmt = $conn->query( $stmt );
        while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC ) ) {
            $trans_id = $row['trans_id'];
            unset($row['trans_id']);
            if (key_exists('points_balance', $row)) {
                array_push( $transactions[$trans_id]['points'], $row );
            }
        }

        $stmt = "select * from (
					select t.id trans_id, '4' payment_type, cc.card_number_ident payment_code,tp.amount payment_amount, tp.credit_card_num 
					FROM TCPOS4.dbo.transactions t 
						join TCPOS4.dbo.tills ts on t.till_id = ts.id 
						join TCPOS4.dbo.shops sh on t.shop_id =sh.id
						join TCPOS4.dbo.trans_payments tp on t.id = tp.transaction_id 
						join TCPOS4.dbo.credit_cards cc on tp.credit_card_id =cc.id 
					where tp.credit_card_id is not null and convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null
					union all
					select t.id trans_id,
					    '0' payment_type,
						case 
					        when tp.payment_id = 1 then '01'
					        when tp.payment_id = 5 then '04'
					        when tp.payment_id = 10 then '08'
					        when tp.payment_id = 11 then '05'
					        when tp.payment_id = 12 then '08'
					        when tp.payment_id = 13 then '01'
					    else 
					        '00'
					    end payment_code,
						tp.amount payment_amount,
					    '' credit_card_num
					FROM TCPOS4.dbo.transactions t 
						join TCPOS4.dbo.tills ts on t.till_id = ts.id 
						join TCPOS4.dbo.shops sh on t.shop_id =sh.id
						join TCPOS4.dbo.trans_payments tp on t.id = tp.transaction_id 
					where tp.credit_card_id is null and tp.voucher_id is null and convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null
					union all
					SELECT t.id trans_id, '1' payment_type, '04' payment_code, tp.amount payment_amount, '' credit_card_num
					FROM TCPOS4.dbo.transactions t 
					    join TCPOS4.dbo.tills ts on t.till_id = ts.id 
					    join TCPOS4.dbo.shops sh on t.shop_id = sh.id
					    join TCPOS4.dbo.trans_payments tp on t.id = tp.transaction_id 
					where tp.voucher_id is not null and convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null
    			) as a
				order by a.trans_id, a.payment_code";

	    $stmt = $conn->query( $stmt );
	    while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC ) ) {
		    $trans_id = $row['trans_id'];
		    if (key_exists($trans_id, $transactions)) {
			    $payments = [];
			    if (key_exists('payments', $transactions[$trans_id])) {
				    $payments = $transactions[$trans_id]['payments'];
			    }
			    unset($row['trans_id']);

			    $payments[] = $row;

			    $transactions[$trans_id]['payments'] = $payments;
		    }
	    }


	    $stmt = "select t.id trans_id, case when tv.vat_id = 1 then 7 when tv.vat_id = 4 then 1 else tv.vat_id end vat_code, tv.vat_percent, tv.gross_amount, tv.net_amount, tv.vat_amount 
				FROM TCPOS4.dbo.transactions t 
					join TCPOS4.dbo.tills ts on t.till_id = ts.id 
					join TCPOS4.dbo.shops sh on t.shop_id =sh.id
					join TCPOS4.dbo.trans_payments tp on t.id = tp.transaction_id 
					join TCPOS4.dbo.trans_vats tv on t.id = tv.transaction_id 
				where convert(DATE, t.trans_date) = '$data' $tillSearch and t.delete_timestamp is null";

	    $stmt = $conn->query( $stmt );
	    while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC ) ) {
		    $trans_id = $row['trans_id'];
		    if (key_exists($trans_id, $transactions)) {
			    $transaction_vat = [];
			    if (key_exists('transaction_vat', $transactions[$trans_id])) {
				    $transaction_vat = $transactions[$trans_id]['transaction_vat'];
			    }
			    unset($row['trans_id']);

			    $transaction_vat[] = $row;

			    $transactions[$trans_id]['transaction_vat'] = $transaction_vat;
		    }
	    }

        return json_encode($transactions);
    }

    public function creazioneDatacollectEpipoli(array $request):string {

        $codiceCampagna = '10501';

        $codicePromozione = [
            '3' => '990011425', //3-MENU KIDS 2020
            '4' => '990011426', //4-MENU HAMBURGER 2020
            '5' => '990011427', //5-MENU CLASSICO 2020
            '8' => '990011428', //8-MENU PRIMO 2020
            '12' => '990011429', //12-MENU SPECIALE 2020
            '13' => '990011430', //13-MENU GOURMET 2020
            '14' => '990011431', //14-MENU SECONDO DI CARNE 2020
            '15' => '990011432', //15-MENU SECONDO DI PESCE 2020
            '11' => '990011437' //11-MENU KIDS 2020
        ];

        //$dataCollectTcPos = $this->creazioneDatacollectTcPos( $request );
        $dataCollectTcPos = file_get_contents('/Users/if65/Desktop/test/tcpos_20200729.json');

        $transactions = json_decode($dataCollectTcPos,true);

        $dc = [];
        $numRec = 0;
        $matches = [];

        if (preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $request['data'], $matches)) {
            $dataRiferimento = $matches[1].$matches[2].$matches[3];

            foreach ($transactions as $transaction) {
                if ($transaction['trans_num'] == '0011') {
                    echo "\n";
                }
                $ora = '';
                if (preg_match('/^.{10}T(\d{2}):(\d{2}):(\d{2})$/', $transaction['trans_date'], $matches)) {
                    $ora = $matches[1].$matches[2];
                }
                $cardNum = '';
                if ($transaction['card_num'] != null) {
                    $cardNum = $transaction['card_num'];
                }
                $dc[] = sprintf( '%08s%08d%-5s004%04d%04d%06d%08d%04s%13s%1s%45s',
                    $dataRiferimento,
                    ++$numRec,
                    '0500',//$request['sede'],
                    $transaction['trans_num'],
                    $transaction['till_code'],
                    $transaction['operator_code'],
                    $dataRiferimento,
                    $ora,
                    $cardNum,
                    0,
                    ''
                );

                // vendita
                foreach ($transaction['articles'] as $article) {


                    if ($article['article_price'] <> 0) {

                        $totaleBuoniPasto = 0;
                        if ($article['article_code'] == '0000-0001' or $article['article_code'] == '0000-0001') {
                            $totaleBuoniPasto += round($article['article_price'], 2);
                        }
                        $prezzo = round( $article['article_price'] + $article['price_article_menu_addition'] + $article['discount'], 2 );
                        $prezzoListino = round($article['article_catalog_price_unit'] * $article['quantity'] ,2);
                        $sconto = 0;
                        if (round( $prezzoListino - $prezzo, 2 ) and ($article['menu_id'] != null)) {
                            $sconto = round( $prezzoListino - $prezzo, 2 );
                        } else {
                            $prezzoListino = $prezzo;
                        }

                        $dc[] = sprintf( '%08s%08s%-5s1001%13s%1s%4s%09d%1d%09d%9s%9s%02d%-10s%13s%1d   ',
                            $dataRiferimento,
                            ++$numRec,
                            '0500',//$request['sede'],
                            $article['article_barcode'],
                            'N',
                            '',
                            round( round( $prezzoListino , 2 ) * 100, 0 ),
                            0,
                            round( $article['quantity'] * 1000, 0 ),
                            '',
                            '',
                            0,
                            $article['article_code'],
                            '',
                            0
                        );

                        if ($sconto != 0) {
                            $dc[] = sprintf( '%08s%08s%-5s1091%13s%1s%4s%09d%1d%09d%-9s%9s%02d%-10s%13s%1d   ',
                                $dataRiferimento,
                                ++$numRec,
                                '0500',//$request['sede'],
                                $article['article_barcode'],
                                'N',
                                '',
                                round( $sconto * 100, 0 ),
                                0,
                                round( 0, 0 ),
                                $codiceCampagna,
                                ($article['menu_id'] != null) ? $codicePromozione[$article['menu_id']] : '',
                                0,
                                '',
                                '',
                                0
                            );
                        }

                    }
                }
                // punti transazione
                foreach ($transaction['points'] as $point) {
                    $dc[] = sprintf( '%08s%08s%-5s1077%18s%09d%1d%09d%-9s%9s%02d%-10s%13s%1d',
                        $dataRiferimento,
                        ++$numRec,
                        '0500',//$request['sede'],
                        '',
                        round($point['points_gained'] - $point['points_used'] , 0),
                        0,
                        0,
                        10485,
                        990011267,
                        0,
                        '',
                        '',
                        0
                    );
                }

                // chiusura transazione
                $dc[] = sprintf( '%08s%08s%-5s1020%18s%09d%1d%09d%9s%9s%02d%-10s%13s%1d   ',$dataRiferimento,
                    ++$numRec,
                    '0500',//$request['sede'],
                    '',
                    round(($transaction['total_amount'] - $totaleBuoniPasto) * 100, 0),
                    0,
                    0,
                    '',
                    '',
                    0,
                    '',
                    '',
                    0
                );
            }
        }

        return implode("\r\n", $dc) . "\r\n";
    }

    public function creazioneDatacollectRiepvegi(array $request):string {
        $dataCollectTcPos = $this->creazioneDatacollectTcPos($request);

        $transactions = json_decode($dataCollectTcPos,true);

        $result = [];
        foreach ($transactions as $transaction) {
            foreach ($transaction['articles'] as $article) {
                $quantita = round($article['quantity'],2);
                $venduto_alle_casse = round($article['article_price'] + $article['price_article_menu_addition'] + $article['discount'],2);

                $venduto_a_listino = round($article['article_catalog_price_unit'] * $quantita,2);
                $articolo_in_offerta = false;
                if (round($article['article_price'] + $article['price_article_menu_addition'] - $venduto_a_listino, 2) != 0.00) {
                    $articolo_in_offerta = true;
                }
                $articolo_in_sconto = false;
                if (round($article['discount'], 2) != 0.00) {
                    $articolo_in_sconto = true;
                }

                if (key_exists($article['article_code'], $result)) {
                    $selectedArticle = $result[$article['article_code']];

                    $selectedArticle['quantita'] += $quantita;
                    $selectedArticle['quantita_in_offerta'] += ($articolo_in_offerta) ? $quantita : 0.00;
                    $selectedArticle['quantita_in_sconto'] += ($articolo_in_sconto) ? $quantita : 0.00;
                    $selectedArticle['venduto'] = round($selectedArticle['venduto'] + $venduto_alle_casse, 2);
                    $selectedArticle['venduto_in_offerta'] = round($selectedArticle['venduto_in_offerta'] + (($articolo_in_offerta) ? round($venduto_alle_casse, 2) : 0.00), 2);
                    $selectedArticle['venduto_in_sconto'] = round($selectedArticle['venduto_in_sconto'] + (($articolo_in_sconto) ? round($article['discount'], 2) : 0.00), 2);
                    $selectedArticle['venduto_a_listino'] = round($selectedArticle['venduto_a_listino'] + round($venduto_a_listino,2), 2);

                    $result[$article['article_code']] = $selectedArticle;
                } else {
                    $result[$article['article_code']] = [
                        'sede' => $request['sede'],
                        'data' => $request['data'],
                        'quantita' => $quantita,
                        'quantita_in_offerta' => ($articolo_in_offerta) ? $quantita : 0.00,
                        'quantita_in_sconto' => ($articolo_in_sconto) ? $quantita : 0.00,
                        'venduto' => $venduto_alle_casse,
                        'venduto_in_offerta' => ($articolo_in_offerta) ? $venduto_alle_casse : 0.00,
                        'venduto_in_sconto' => ($articolo_in_sconto) ? round($article['discount'], 2) : 0.00,
                        'venduto_a_listino' => $venduto_a_listino,
                    ];
                }
            }
        }

        return json_encode($result);
    }

    public function creazioneDatacollectRiepvegiTxt(array $request, string $dati) : string {

        $societa = '';
        $negozio = '';
        if (preg_match('/^(\d\d)(\d\d)$/', $request['sede'], $matches)) {
            $societa = $matches[1];
            $negozio = $matches[2];
        }

        $data = $request['data'];
        $giorno = '';
        $mese = '';
        $anno = '';
        if (preg_match('/^\d\d(\d\d)\-(\d\d)\-(\d\d)$/', $request['data'], $matches)) {
            $anno = $matches[1];
            $mese = $matches[2];
            $giorno = $matches[3];
        }
        $datacollect = json_decode($dati, true);

        $riepvegi = [];
        foreach($datacollect as $code => $article) {
            $row = '';
            $row .= $societa . "\t";
            $row .= $negozio . "\t";
            $row .= $code . "\t";
            $row .= (key_exists('barcode', $article) ? $article['barcode'] : '') . "\t";
            $row .= $data . "\t";
            $row .= $anno . "\t";
            $row .= $mese . "\t";
            $row .= $giorno . "\t";
            $row .= $article['quantita'] . "\t";
            $row .= $article['quantita_in_offerta'] . "\t";
            $row .= $article['quantita_in_sconto'] . "\t";
            $row .= "0.00" . "\t";
            $row .= "0.00" . "\t";
            $row .= "0.00" . "\t";
            $row .= "0.00" . "\t";
            $row .= "0.00" . "\t";
            $row .= 'L' . "\t"; // segno tipo prezzo
            $row .= '0' . "\t"; // forzaprezzo
            $row .= '' . "\t"; //segno
            $row .= '' . "\t";//segno
            $row .= '' . "\t";//segno
            $row .= '' . "\t";//segno
            $row .= "0.00" . "\t";//filler
            $row .= $article['venduto'] . "\t";
            $row .= $article['venduto_a_listino']  . "\t";
            $row .= $article['venduto'] . "\t";
            $row .= $article['venduto_in_offerta']  . "\t";
            $row .= abs($article['venduto_in_sconto'])  . "\t";
            $row .= '' . "\t";
            $row .= '' . "\t";
            $row .= '' . "\t";
            $row .= '' . "\t";
            $row .= '' . "\t";
            $row .= '' . "\t";
            $row .= '' . "\t";
            $row .= '' ;

            $riepvegi[] = $row;
        }

        return implode("\n", $riepvegi) . "\n";
    }

    public function incassiInTempoReale(array $request): array {
        $data = $request['data'];

        $conn = new \PDO("sqlsrv:Server=".$this->hostname.",9089;Database=".$this->dbname, $this->username, $this->password);

        $conn->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
        $conn->setAttribute( \PDO::SQLSRV_ATTR_QUERY_TIMEOUT, 10 );

        $stmt = "SELECT 
				    case 
				        when left(sh.code, 4) = '0600' then '0502' 	
				        when left(sh.code, 4) = '0700' then '0503' 
				    else 
				        left(sh.code, 4) 
				    end store,
				    convert(DATE, t.trans_date) ddate,
				    sum(t.total_amount) totalamount,
				    count(*) customerCount
				FROM TCPOS4.dbo.transactions t 
				    join TCPOS4.dbo.tills ts on t.till_id = ts.id 
				    join TCPOS4.dbo.shops sh on t.shop_id = sh.id 
				where convert(DATE, t.trans_date) = '$data'
				group by case 
				        when left(sh.code, 4) = '0600' then '0502' 	
				        when left(sh.code, 4) = '0700' then '0503' 
				    else 
				        left(sh.code, 4) 
				    end,
    				convert(DATE, t.trans_date)";

        $result = [];
        $stmt = $conn->query( $stmt );
        while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC )) {
	        $result[] = $row;
        }

        return $result;
    }

    public function recuperaFatture(): string {
        try {
            $conn = new \PDO( "sqlsrv:Server=" . $this->hostname . ",9089;Database=" . $this->dbname, $this->username, $this->password );

            $conn->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
            $conn->setAttribute( \PDO::SQLSRV_ATTR_QUERY_TIMEOUT, 10 );

            $conn->query( "update TCPOS4.dbo.transactions set exported = 10 where fiscal_invoice is not NULL and exported = 0;" );

            $sql = "    select 
                        t.id,
                        case 
                            when left(s.code, 4) = '0500' then '0501'
        					when left(s.code, 4) = '0600' then '0502' 	
        					when left(s.code, 4) = '0700' then '0503' 
    					else 
        					left(s.code, 4) 
    					end shop_code,
                        t2.code till_code,
                        t2.description, 
                        t.fiscal_invoice invoice_num, 
                        convert(DATE, t.invoice_date) 'invoice_date',
                        convert(DATE, t.trans_date) 'trans_date',
                        convert(TIME, t.trans_date) 'trans_time',
                        t.trans_num, 
                        tfd.name, 
                        tfd.surname, 
                        tfd.fiscal_code, 
                        tfd.fiscal_vat_number, 
                        tfd.taxres_street, 
                        tfd.taxres_ZIP, 
                        tfd.taxres_city, 
                        tfd.taxres_district, 
                        tfd.taxres_country,
                        isnull(tfd.sdi_identification,'0000000') sdi_identification
                    from TCPOS4.dbo.transactions t
                        join TCPOS4.dbo.trans_fiscal_data tfd on t.id = tfd.transaction_id
                        join TCPOS4.dbo.shops s on t.shop_id = s.id
                        join TCPOS4.dbo.tills t2 on t.till_id = t2.id 
                    where t.fiscal_invoice is not NULL and t.exported = 10;";

            $fatture = [];
            $stmt = $conn->query( $sql );
            while ($row = $stmt->fetch( \PDO::FETCH_ASSOC )) {
                $row['rows'] = [];
                $row['vats'] = [];
                $row['payments'] = [];
                if ($row['shop_code'] == '0501') {
                    $row['invoice_num'] = '51-' . $row['invoice_num'];
                } elseif ($row['shop_code'] == '0502') {
                    $row['invoice_num'] = '52-' . $row['invoice_num'];
                } elseif ($row['shop_code'] == '0503') {
	                $row['invoice_num'] = '53-' . $row['invoice_num'];
                } elseif ($row['shop_code'] == '0201') {
	                $row['invoice_num'] = '51-' . $row['invoice_num'];
                } elseif ($row['shop_code'] == '0155') {
	                $row['invoice_num'] = '52-' . $row['invoice_num'];
                } elseif ($row['shop_code'] == '0142') {
	                $row['invoice_num'] = '53-' . $row['invoice_num'];
                } elseif ($row['shop_code'] == '0203') {
	                $row['invoice_num'] = '54-' . $row['invoice_num'];
                } else {
                    $row['invoice_num'] = '00-' . $row['invoice_num'];
                }
                $fatture[$row['id']] = $row;
            }


            $sql = "    select 
                        t.id,
                        ta.hash_code, 
                        a.code,
                        a.description,
                        ta.qty_weight,
                        ta.price + COALESCE(ta.discount, 0) amount,
                        case 
                            when v.id = 1 then 7
                            when v.id = 2 then 2
                            when v.id = 3 then 3
                            when v.id = 4 then 1
                        else       
                            ''
                        end vat_code,
                        ta.vat_percent 
                    from TCPOS4.dbo.transactions t
                        join TCPOS4.dbo.trans_articles ta on t.id = ta.transaction_id 
                        join TCPOS4.dbo.articles a on ta.article_id = a.id
                        join TCPOS4.dbo.vats v on ta.vat_id = v.id 
                    where t.fiscal_invoice is not NULL and ta.delete_timestamp is null and t.exported = 10
                    order by t.id, ta.hash_code;";

            $stmt = $conn->query( $sql );
            while ($row = $stmt->fetch( \PDO::FETCH_ASSOC )) {
                if (key_exists( $row['id'], $fatture )) {
                    $row['amount'] *= 1;
                    $row['vat_percent'] *= 1;
                    $row['qty_weight'] *= 1;
                    $net_amount = round( $row['amount'] / ($row['vat_percent'] + 100) * 100, 2 );
                    $vat_amount = round( $row['amount'] - $net_amount, 2 );
                    $row['vat_amount'] = $vat_amount;
                    $row['net_amount'] = $net_amount;
                    $fatture[$row['id']]['rows'][] = $row;
                }
            }

            $sql = "    select 
                        t.id,
                        case 
                            when v.id = 1 then 7
                            when v.id = 2 then 2
                            when v.id = 3 then 3
                            when v.id = 4 then 1
                        else       
                            ''
                        end vat_code,
                        v.vat_percent,
                        tv.gross_amount,
                        tv.vat_amount,
                        tv.net_amount 
                    from TCPOS4.dbo.transactions t
                        join trans_vats tv on t.id = tv.transaction_id 
                        join TCPOS4.dbo.vats v on tv.vat_id = v.id 
                    where t.fiscal_invoice is not NULL and t.exported = 10;";

            $stmt = $conn->query( $sql );
            while ($row = $stmt->fetch( \PDO::FETCH_ASSOC )) {
                if (key_exists( $row['id'], $fatture )) {
                    $fatture[$row['id']]['vats'][] = $row;
                }
            }


            $sql = "    select 
                        t.id,
                        tp.amount,
                        p.code payment_code,
                        p.description 
                    from TCPOS4.dbo.transactions t
                        join trans_payments tp on t.id = tp.transaction_id 
                        join payments p on tp.payment_id = p.id
                    where t.fiscal_invoice is not NULL and t.exported = 10;";

            $stmt = $conn->query( $sql );
            while ($row = $stmt->fetch( \PDO::FETCH_ASSOC )) {
                if (key_exists( $row['id'], $fatture )) {
                    $fatture[$row['id']]['payments'][] = $row;
                }
            }

            $sql = "    select 
                        t.id,
                        sum(tv.vat_amount ) vat_amount,
                        sum(tv.net_amount ) net_amount,
                        sum(tv.gross_amount ) gross_amount
                    from TCPOS4.dbo.transactions t
                        join TCPOS4.dbo.trans_vats tv on t.id = tv.transaction_id 
                    where t.fiscal_invoice is not NULL and t.exported = 10
                    group by t.id;";

            $stmt = $conn->query( $sql );
            while ($row = $stmt->fetch( \PDO::FETCH_ASSOC )) {
                if (key_exists( $row['id'], $fatture )) {
                    $fatture[$row['id']]['vat_amount'] = $row['vat_amount'] * 1;
                    $fatture[$row['id']]['net_amount'] = $row['net_amount'] * 1;
                    $fatture[$row['id']]['gross_amount'] = $row['gross_amount'] * 1;
                }
            }

            $conn->query( "update TCPOS4.dbo.transactions set exported = 20 where fiscal_invoice is not NULL and exported = 10;" );

            return json_encode( $fatture );
        } catch (PDOException $e) {
            return '';
        }
    }

    public function creaFileInterscambioFatture(string $fattureJson): string {
        $fatture = json_decode($fattureJson, true);

        $righe = [];
        $contatore = 0;
        foreach ($fatture as $id => $fattura) {
	        $codiceSocietaEmittente = '02';
	        $ragioneSocialeSocietaEmittente = 'ITALMARK S.R.L.';
	        $partitaIvaSocietaEmittente = '04145590982';
        	if ($fattura['shop_code'] == '0501') {
				$codiceSocietaEmittente = '05';
		        $ragioneSocialeSocietaEmittente = 'IT\'S MARKET S.R.L.';
		        $partitaIvaSocietaEmittente = '04130570981';
	        } elseif ($fattura['shop_code'] == '0502') {
		        $codiceSocietaEmittente = '05';
		        $ragioneSocialeSocietaEmittente = 'IT\'S MARKET S.R.L.';
		        $partitaIvaSocietaEmittente = '04130570981';
	        } elseif ($fattura['shop_code'] == '0503') {
		        $codiceSocietaEmittente = '05';
		        $ragioneSocialeSocietaEmittente = 'IT\'S MARKET S.R.L.';
		        $partitaIvaSocietaEmittente = '04130570981';
	        };

            $righe[] = sprintf('%s;%s;%04d;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%d;%d;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;',
                '100', // tipo record
                $fattura['invoice_num'], // numero fattura
                ++$contatore, // progressivo
                str_replace('-', '', $fattura['invoice_date']), // data fattura
	            $codiceSocietaEmittente, // codice società emittente
	            $ragioneSocialeSocietaEmittente, // ragione sociale emittente
	            $partitaIvaSocietaEmittente, // partita iva emittente
                '', // codice societa destinataria (qc_ced_clienti)
                trim(trim ($fattura['name'], " \t\n\r\0\x0B") . ' ' . trim ($fattura['surname'], " \t\n\r\0\x0B")), // ragione sociale destinatario
                trim($fattura['fiscal_code']," \t\n\r\0\x0B"), // codice fiscale destinatario
                trim($fattura['fiscal_vat_number']," \t\n\r\0\x0B"), // partita iva destinatario
                '', // codice rapporto cliente
                '', // codice cliente
                0, // split payment
                0, // pubblica amministrazione
                'TD01', // tipo documento
                'EUR', // divisa
                '', // codice commessa convenzionale
                trim($fattura['sdi_identification']," \t\n\r\0\x0B"), // codice CUP
                '', // codice CIG
                $fattura['shop_code'], // codice negozio emittente
                trim($fattura['taxres_street']," \t\n\r\0\x0B"), // indirizzo cliente
                trim($fattura['taxres_ZIP']," \t\n\r\0\x0B"), // cap cliente
                trim($fattura['taxres_city']," \t\n\r\0\x0B"), // città cliente
                trim($fattura['taxres_district']," \t\n\r\0\x0B"), // provincia cliente
                trim($fattura['taxres_country']," \t\n\r\0\x0B"), // stato cliente
                '0000000', // codice destinatario
                '', // tipo movimento coge mitico
                '', // conto ricavo mitico
                '', // tipo ritenuta
                '', // importo ritenuta
                '', // aliquota ritenuta
                '', // causale pagamento ritenuta
                '', // convenzione id documento
                '' // data conv id documento
            );

            $righe[] = sprintf('%s;%s;%04d;%s;',
                '300', // tipo record
                $fattura['invoice_num'], // numero fattura
                ++$contatore, // progressivo
                '' // causale campo libero
            );

            foreach ($fattura['rows'] as $riga) {
                $righe[] = sprintf('%s;%s;%04d;%s;%s;%s;%s;%s;%s;%s;%s;%.2f;%.2f;%.2f;%d;%s;%s;%.2f;%.2f;%.2f;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;',
                    '500', // tipo record
                    $fattura['invoice_num'], // numero fattura
                    ++$contatore, // progressivo
                    'codice interno', // tipo codice 1
                    $riga['code'], // valore codice 1
                    '', // tipo codice 2
                    '', // valore codice 2
                    '', // tipo codice 3
                    '', // valore codice 3
                    $riga['description'], // descrizione articolo
                    '', // unità misura articolo
                    $riga['qty_weight'], // quantita
                    $riga['qty_weight'] != 0.00 ? $riga['amount'] / $riga['qty_weight'] : 0.00, // prezzo unitario
                    $riga['vat_percent'], // aliquota iva
                    $riga['vat_code'], // codice iva
                    '', // id documento
                    '', // data documento
                    $riga['net_amount'], // imponibile
                    $riga['amount'], // imponibile + imposta
                    $riga['vat_amount'], // imposta
                    '0501', // codice negozio emittente
                    '', // codice negozio ricevente
                    '', // id documento
                    '', // data documento
                    '', // numitem
                    '', // tipo cessione
                    '', // tipo sconto maggiorazione
                    '', // perc sconto maggiorazione
                    '', // importo sconto maggiorazione
                    'NUMERO SCONTRINO', // altri dati gestionali tipo
                    $fattura['trans_num'] . '/' . $fattura['till_code'], // altri dati gestionali riferim. testo
                    $fattura['trans_num'], // altri dati gestionali riferim. num.
                    str_replace('-', '', $fattura['trans_date']), // altri dati gestionali riferim. data
                    '', // altri dati gestionali tipo 2
                    '', // altri dati gestionali riferim. testo 2
                    '', // altri dati gestionali riferim. num. 2
                    '', // altri dati gestionali riferim. data 2
                    '',
                    '',
                    ''
                );
            }

            foreach ($fattura['vats'] as $riga) {
                $righe[] = sprintf( '%s;%s;%04d;%s;%.2f;%d;%.2f;%.2f;',
                    '800', // tipo record
                    $fattura['invoice_num'], // numero fattura
                    ++$contatore, // progressivo
                    str_replace( '-', '', $fattura['invoice_date'] ), // data fattura
                    $riga['vat_percent'], // aliquota iva
                    $riga['vat_code'], // codice iva
                    $riga['net_amount'], // imponibile
                    $riga['vat_amount'] // imposta
                );
            }

            $righe[] = sprintf( '%s;%s;%04d;%.2f;%.2f;%.2f;%s;%s;%s;%s;%s;%s;%s;%s;%s;%.2f,%s;%s;%s;',
                '900', // tipo record
                $fattura['invoice_num'], // numero fattura
                ++$contatore, // progressivo
                $fattura['net_amount'], // imponibile
                $fattura['vat_amount'], // imposta
                $fattura['gross_amount'], // totale fattura
                '', // tipo sconto maggiorazione
                '', // percentuale sconto maggiorazione
                '', // importo sconto maggiorazione
                'TP02', // condizioni di pagamento
                'MP01', // modalita di pagamento
                str_replace('-', '', $fattura['trans_date']), // data riferimento termini di pagamento
                '', // giorni termini di pagamento
                '', // data scadenza pagamento
                '', // iban
                0, // importo da pagare
                '', // email
                '', // numero telefono
                '' // codice univoco
            );

        }

        if (count($righe)) {
            $righe[] = sprintf( '%s;%s;%s;',
                '999', // tipo record
                date( 'Ymd' ), // data elaborazione
                date( 'His' ) // ora elaborazione
            );
        }

        return implode("\r\n", $righe);
    }

    public function elencoFattureEmesse(): string {

        $conn = new \PDO("sqlsrv:Server=".$this->hostname.",9089;Database=".$this->dbname, $this->username, $this->password);

        $conn->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
        $conn->setAttribute( \PDO::SQLSRV_ATTR_QUERY_TIMEOUT, 10 );

        $conn->query( "update TCPOS4.dbo.transactions set exported = 0 where fiscal_invoice is not NULL and exported is NULL;" );

        $sql = "    select 
                        t.id,
                        t.fiscal_invoice invoice_num,
                        convert(date, t.invoice_date) invoice_date,
                        convert(time, t.invoice_date) invoice_time,
                         case 
                            when left(s.code, 4) = '0500' then '0501' 
        					when left(s.code, 4) = '0600' then '0502' 	
        					when left(s.code, 4) = '0700' then '0503' 
    					else 
        					left(s.code, 4) 
    					end shop_code,
                        isnull(tfd.name,'') name, 
                        isnull(tfd.surname,'') surname, 
                        isnull(tfd.fiscal_code,'') fiscal_code,
                        isnull(tfd.fiscal_vat_number,'') fiscal_vat_number, 
                        isnull(tfd.taxres_street,'') taxres_street, 
                        isnull(tfd.taxres_ZIP,'') taxres_ZIP, 
                        isnull(tfd.taxres_city,'') taxres_city, 
                        isnull(tfd.taxres_district,'') taxres_district, 
                        isnull(tfd.taxres_country,'') taxres_country,
                        isnull(tfd.sdi_identification,'0000000') sdi_identification,
                        isnull(t.total_amount,0) total_amount
                    from TCPOS4.dbo.transactions t
                        join TCPOS4.dbo.trans_fiscal_data tfd on t.id = tfd.transaction_id
                        join TCPOS4.dbo.shops s on t.shop_id = s.id
                        join TCPOS4.dbo.tills t2 on t.till_id = t2.id 
                    where t.fiscal_invoice is not NULL and t.exported = 0;";

        $elencoFatture = [];

        $stmt = $conn->query( $sql );
        while ( $row = $stmt->fetch( \PDO::FETCH_ASSOC )) {
            if ($row['shop_code'] == '0501') {
                $row['invoice_num'] = '51-' . $row['invoice_num'];
            } elseif ($row['shop_code'] == '0502') {
                $row['invoice_num'] = '52-' . $row['invoice_num'];
            }  elseif ($row['shop_code'] == '0503') {
	            $row['invoice_num'] = '53-' . $row['invoice_num'];
            }  elseif ($row['shop_code'] == '0201') {
	            $row['invoice_num'] = '51-' . $row['invoice_num'];
            }  elseif ($row['shop_code'] == '0155') {
	            $row['invoice_num'] = '52-' . $row['invoice_num'];
            }  elseif ($row['shop_code'] == '0142') {
	            $row['invoice_num'] = '53-' . $row['invoice_num'];
            }  elseif ($row['shop_code'] == '0203') {
	            $row['invoice_num'] = '54-' . $row['invoice_num'];
            } else {
                $row['invoice_num'] = '00-' . $row['invoice_num'];
            }

            $elencoFatture[] = $row;
        }

        return json_encode($elencoFatture);
    }

    public function testRiepvegi(array $request, $dataCollectTcPos):string {

        $transactions = json_decode($dataCollectTcPos,true);

        $result = [];
        foreach ($transactions as $transaction) {
            foreach ($transaction['articles'] as $article) {
                $quantita = round($article['quantity'],2);
                $venduto_alle_casse = round(round($article['article_price'],2) +
                    round($article['price_article_menu_addition'],2) +
                    round($article['discount'], 2),2);
                $venduto_a_listino = round(round($article['article_catalog_price_unit'],2) * $quantita,2);
                $articolo_in_offerta = false;
                if (round($venduto_alle_casse - $venduto_a_listino, 2) != 0.00) {
                    $articolo_in_offerta = true;
                }
                $articolo_in_sconto = false;
                if (round($article['discount'], 2) != 0.00) {
                    $articolo_in_sconto = true;
                }

                if (key_exists($article['article_code'], $result)) {
                    $selectedArticle = $result[$article['article_code']];

                    $selectedArticle['quantita'] += $quantita;
                    $selectedArticle['quantita_in_offerta'] += ($articolo_in_offerta) ? $quantita : 0.00;
                    $selectedArticle['quantita_in_sconto'] += ($articolo_in_sconto) ? $quantita : 0.00;
                    $selectedArticle['venduto'] = round($selectedArticle['venduto'] + $venduto_alle_casse, 2);
                    $selectedArticle['venduto_in_offerta'] = round($selectedArticle['venduto_in_offerta'] + ($articolo_in_offerta) ? round($venduto_alle_casse, 2) : 0.00, 2);
                    $selectedArticle['venduto_in_sconto'] = round($selectedArticle['venduto_in_sconto'] + ($articolo_in_sconto) ? round($article['discount'], 2) : 0.00, 2);
                    $selectedArticle['venduto_a_listino'] = round($selectedArticle['venduto_a_listino'] + round($venduto_a_listino,2), 2);

                    $result[$article['article_code']] = $selectedArticle;
                } else {
                    $result[$article['article_code']] = [
                        'sede' => $request['sede'],
                        'data' => $request['data'],
                        'quantita' => $quantita,
                        'quantita_in_offerta' => ($articolo_in_offerta) ? $quantita : 0.00,
                        'quantita_in_sconto' => ($articolo_in_sconto) ? $quantita : 0.00,
                        'venduto' => $venduto_alle_casse,
                        'venduto_in_offerta' => ($articolo_in_offerta) ? $venduto_alle_casse : 0.00,
                        'venduto_in_sconto' => ($articolo_in_sconto) ? round($article['discount'], 2) : 0.00,
                        'venduto_a_listino' => $venduto_a_listino,
                    ];
                }
            }
        }

        return json_encode($result);
    }
}