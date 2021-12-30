<?php
namespace Database\Tabelle;

class TIdc extends TTable {

    public function __construct($pdo, $schema, $tableName) {
        parent::__construct($pdo, $schema, $tableName);

        self::creaTabella(
            "CREATE TABLE IF NOT EXISTS `".$this->schema."`.`".$this->tableName."` (
                      `reg` varchar(3) NOT NULL DEFAULT '',
                      `store` varchar(4) NOT NULL DEFAULT '',
                      `ddate` date NOT NULL,
                      `ttime` varchar(6) NOT NULL DEFAULT '000000',
                      `hour` varchar(2) NOT NULL,
                      `sequencenumber` int(11) unsigned NOT NULL,
                      `trans` smallint(5) unsigned NOT NULL,
                      `transstep` smallint(5) unsigned NOT NULL,
                      `recordtype` varchar(1) NOT NULL DEFAULT '',
                      `recordcode1` varchar(1) NOT NULL DEFAULT '',
                      `recordcode2` varchar(1) NOT NULL,
                      `recordcode3` varchar(1) NOT NULL,
                      `userno` smallint(5) unsigned NOT NULL,
                      `misc` varchar(16) NOT NULL DEFAULT '',
                      `data` varchar(19) NOT NULL DEFAULT '',
                      `saleid` smallint(5) unsigned NOT NULL DEFAULT 0,
                      `taxcode` tinyint(3) unsigned NOT NULL DEFAULT 0,
                      `amount` decimal(11,2) NOT NULL DEFAULT 0.00,
                      `totalamount` decimal(11,2) NOT NULL DEFAULT 0.00,
                      `totaltaxableamount` decimal(11,2) NOT NULL DEFAULT 0.00,
                      `taxamount` decimal(11,2) NOT NULL DEFAULT 0.00,
                      `barcode` varchar(13) NOT NULL DEFAULT '',
                      `quantita` decimal(7,3) NOT NULL DEFAULT 0.000,
                      `totalpoints` smallint(6) NOT NULL,
                      `paymentform` varchar(2) NOT NULL DEFAULT '',
                      `created_at` timestamp NULL DEFAULT NULL,
                      PRIMARY KEY (`store`,`ddate`,`sequencenumber`),
                      KEY `recordtype` (`recordtype`),
                      KEY `store` (`store`,`ddate`),
                      KEY `barcode` (`barcode`),
                      KEY `created_at` (`created_at`),
                      KEY `store_2` (`store`,`ddate`,`reg`,`trans`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        );
    }

    public function creaRecord(array $incarico): string {
        /*try {
            $sql = "insert into `" . $this->schema . "`.`" . $this->tableName . "` 
                            (`id`,`idPadre`,`codicePromozione`,`lavoroCodice`,`lavoroDescrizione`,`negozioCodice`,`negozioDescrizione`,`stato`,`tsPianificazione`,`tsEsecuzione`)
                    values
                            (:id,:idPadre,:codicePromozione,:lavoroCodice,:lavoroDescrizione,:negozioCodice,:negozioDescrizione,:stato,:tsPianificazione,:tsEsecuzione);";
            $stmt = $this->pdo->prepare( $sql );
            $stmt->execute( [
                'id' => $incarico['id'],
                'idPadre' => $incarico['idPadre'],
                'codicePromozione' => $incarico['codicePromozione'],
                'lavoroCodice' => $incarico['lavoroCodice'],
                'lavoroDescrizione' => $incarico['lavoroDescrizione'],
                'negozioCodice' => $incarico['negozioCodice'],
                'negozioDescrizione' => $incarico['negozioDescrizione'],
                'stato' => key_exists( 'stato', $incarico ) ? $incarico['stato'] : 0,
                'tsPianificazione' => key_exists( 'tsPianificazione', $incarico ) ? $incarico['tsPianificazione'] : null,
                'tsEsecuzione' => key_exists( 'tsEsecuzione', $incarico ) ? $incarico['tsEsecuzione'] : null
            ] );
            $id = $this->pdo->lastInsertId();
            return $id;
        } catch (PDOException $e) {
            //die( $e->getMessage() );
            return '';
        }*/
         return '';
    }

    public function modificaRecord(array $incarico) :bool {
        /*try {
            $sql = "update `$this->schema`.`$this->tableName` 
                set
                    `idPadre`=:idPadre,
                    `codicePromozione`=:codicePromozione,
                    `lavoroCodice`=:lavoroCodice,
                    `lavoroDescrizione`=:lavoroDescrizione,
                    `negozioCodice`=:negozioCodice,
                    `negozioDescrizione`=:negozioDescrizione,
                    `stato`=:stato,
                    `tsPianificazione`=:tsPianificazione,
                    `tsEsecuzione`=:tsEsecuzione
                where 
                    `id` = :id;";
            $stmt = $this->pdo->prepare( $sql );
            $stmt->execute( [
                'idPadre' => $incarico['idPadre'],
                'codicePromozione' => $incarico['codicePromozione'],
                'lavoroCodice' => $incarico['lavoroCodice'],
                'lavoroDescrizione' => $incarico['lavoroDescrizione'],
                'negozioCodice' => $incarico['negozioCodice'],
                'negozioDescrizione' => $incarico['negozioDescrizione'],
                'stato' => $incarico['stato'],
                'tsPianificazione' => $incarico['tsPianificazione'],
                'tsEsecuzione' => $incarico['tsEsecuzione']
            ] );
            return true;

        } catch (PDOException $e) {
            //die( $e->getMessage() );
            return false;
        }*/

        return '';
    }

    public function cancellaRecord(string $id): bool {
        /*try {
            $sql = "delete from `$this->schema`.`$this->tableName` 
                where 
                    `id` = :id and `stato` in (STATO_INDEFINITO, STATO_INVIO_PROMOZIONE);";
            $stmt = $this->pdo->prepare( $sql );
            $stmt->execute( [
                'id' => $id
            ]);
            return true;

        } catch (PDOException $e) {
            //die( $e->getMessage() );
            return false;
        }*/

        return '';
    }

	public function incassiInTempoReale(array $request): array {
    	try {
		    /*$stmt = "	select ddate, store, sum(totalamount) totalamount, count(*) customerCount
						from mtx.idc where ddate = :ddate and recordcode1 = 1 and binary recordtype = 'F' group by 1,2 
						union
						select ddate, store, totalamount, itemCount/2 customerCount 
						from mtx.eod where ddate >= date_sub(:ddate, interval 7 day) and ddate < :ddate order by 1,2";*/
		    $stmt = "	select ddate, store, sum(totalamount) totalamount, count(*) customerCount 
						from mtx.idc where ddate = :ddate and recordcode1 = 1 and binary recordtype = 'F' group by 1,2";
		    $handler = $this->pdo->prepare($stmt);

		    $result = [];
		    if ($handler->execute([':ddate' => $request['data']])) {
			    $result = $handler->fetchAll( \PDO::FETCH_ASSOC );
		    }

		    return $result;
	    } catch (PDOException $e) {
		    return [];
	    }

	}

    public function ricerca(array $daCercare): string {
        return '';
    }

    public function recuperaMTXRigheFatture(array $request):string {
        try {
            $stmt = "   select 
                            b.`CODCIN-BAR2` `code`, 
                            i.barcode, 
                            i.`userno` department, 
                            a.`DES-ART2` description, 
                            i.`taxcode`, 
                            i.`quantita` quantity, 
                            i.`taxamount`, 
                            i.`totalamount`, 
                            i.`totaltaxableamount` 
                        from mtx.idc as i left join archivi.barartx2 as b on case when substr(i.barcode,1,2) in ('21','22','23') then SUBSTR(i.barcode,1,7) else i.barcode end = b.`BAR13-BAR2` left join archivi.articox2 as a on b.`CODCIN-BAR2`=a.`COD-ART2` 
                        where i.store = :store and i.ddate = :ddate and i.reg = :reg and i.trans = :trans and recordcode1 = 1 and binary i.recordtype = 'S';";
            $handler = $this->pdo->prepare($stmt);

            $result = [];
            if ($handler->execute([
                ':store' => $request['store'],
                ':ddate' => $request['ddate'],
                ':reg' => $request['reg'],
                ':trans' => $request['trans']
            ])) {
                $result = $handler->fetchAll( \PDO::FETCH_ASSOC );

            }

            return json_encode(['errorMessage' => 0, 'datacollect' => $result, 'status'=>0]);

        } catch (PDOException $e) {
            return '';
        }
    }

    public function recuperaDatiContabili(array $request): string {
        try {
            $stmt = "   select 
                        concat(lpad(store,4,'0'), DATE_FORMAT(ddate, '%Y%m%d'),lpad(reg,3,'0'),lpad(trans,4,'0')) ID,
                        reg REG, store STORE, ddate DDATE, ttime TTIME,  sequencenumber SEQUENCENUMBER,
                        trans TRANS, transstep TRANSSTEP, recordtype RECORDTYPE, concat(recordcode1,recordcode2,recordcode3) RECORDCODE,
                        userno USERNO, misc MISC, data DATA
                        from `".$this->schema . "`.`" . $this->tableName. "`
                        where binary recordtype in ('F','H','T','V') and recordcode1 = '1' and 
                        ddate = '".$request['data']."' and store = '" . $request['sede'] . "'
                        order by sequencenumber;";
            $handler = $this->pdo->prepare($stmt);

            $result = [];
            if ($handler->execute()) {
                $result = $handler->fetchAll( \PDO::FETCH_ASSOC );

            }

            return json_encode(['errorMessage' => 0, 'datacollect' => $result, 'status'=>0]);

        } catch (PDOException $e) {
            return '';
        }
    }

    public function recuperaDatiPerQuadratura(array $request): string {
        try {
            $tableName = $this->schema . "`.`" . $this->tableName;
            $tableNameEod = $this->schema . "`.`eod";

            $stmt = "   select 'F' tipo, concat(i.store, replace(i.ddate,'-',''), i.reg, lpad(t.userno,3,'0')) id, 0 codice, count(*) s1 , sum(i.`totalamount`) s2 from `$tableName` as i join (select store, ddate, reg, trans, userno from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'H' and recordcode1 = '1') as t on i.store = t.store and i.ddate=t.ddate and i.reg=t.reg and i.trans=t.trans
                        where binary i.recordtype = 'F' group by 1,2,3
                        union
                        select 'A' tipo, concat(store, replace(ddate,'-',''), reg, lpad(userno,3,'0')) id, lpad(trans,4,'0') codice, totalamount s1, ttime s2 from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'T' and recordcode2 = 2 and totalamount<>0
                        union
                        select 'U' tipo, concat(store, replace(ddate,'-',''), reg, lpad(userno,3,'0')) id, lpad(trans,4,'0') codice, totalamount s1, ttime s2 from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'T' and recordcode2 = 5 and totalamount<>0
                        union
                        /*select 'V' tipo, concat(i.store, replace(i.ddate,'-',''), i.reg, lpad(t.userno,3,'0')) id, i.`taxcode` codice, sum(case when t.recordcode2 = '0' then i.`taxamount` else i.`taxamount`* -1 end) s1, sum(case when t.recordcode2 = '0' then i.`amount` else i.`amount`* -1 end) s2  from `$tableName` as i join (select store, ddate, reg, trans, userno, recordcode2 from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'H' and recordcode1 = '1') as t on i.store = t.store and i.ddate=t.ddate and i.reg=t.reg and i.trans=t.trans
                        where binary i.recordtype = 'V' group by 1,2,3*/
                        /*select 'V' tipo, concat(i.store, replace(i.ddate,'-',''), i.reg, lpad(t.userno,3,'0')) id, i.`taxcode` codice, round(sum(case when i.totalamount<0 then i.totaltaxableamount*-1 else i.totaltaxableamount end) - sum(case when i.totalamount<0 then i.taxamount*-1 else i.taxamount end),2) s1, sum(case when i.totalamount<0 then i.taxamount*-1 else i.taxamount end) s2  from `$tableName` as i join (select store, ddate, reg, trans, userno from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'H' and recordcode1 = '1') as t on i.store = t.store and i.ddate=t.ddate and i.reg=t.reg and i.trans=t.trans
                        where binary i.recordtype = 'S' group by 1,2,3*/
   						select 'V' tipo, concat(i.store, replace(i.ddate,'-',''), i.reg, lpad(t.userno,3,'0')) id, i.`taxcode` codice, sum(case when t.recordcode2 = '0' then i.`amount` else i.`amount`* -1 end) s1, sum(case when t.recordcode2 = '0' then i.`taxamount` else i.`taxamount`* -1 end) s2  from `$tableName` as i join (select store, ddate, reg, trans, userno, recordcode2 from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'H' and recordcode1 = '1') as t on i.store = t.store and i.ddate=t.ddate and i.reg=t.reg and i.trans=t.trans
                        where binary i.recordtype = 'V' and  recordcode1 = '1' group by 1,2,3
                        union
   						select 'W' tipo, concat(i.store, replace(i.ddate,'-',''), i.reg, lpad(t.userno,3,'0')) id, i.`taxcode` codice, round(sum(case when i.totalamount<0 then i.totaltaxableamount*-1 else i.totaltaxableamount end) - sum(case when i.totalamount<0 then i.taxamount*-1 else i.taxamount end),2) s1, sum(case when i.totalamount<0 then i.taxamount*-1 else i.taxamount end) s2 from `$tableName` as i join (select store, ddate, reg, trans, userno from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'H' and recordcode1 = '1') as t on i.store = t.store and i.ddate=t.ddate and i.reg=t.reg and i.trans=t.trans
						where binary i.recordtype = 'S' group by 1,2,3
						union
                        select 'T' tipo, concat(i.store, replace(i.ddate,'-',''), i.reg, lpad(t.userno,3,'0')) id, i.`paymentform` codice, sum(i.`totalamount`) s1, 0 s2  from `$tableName` as i join (select store, ddate, reg, trans, userno from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'H' and recordcode1 = '1') as t on i.store = t.store and i.ddate=t.ddate and i.reg=t.reg and i.trans=t.trans
                        where binary i.recordtype = 'T' and  recordcode1 = '1' and recordcode3 <> '2' group by 1,2,3 
                        union
                        select 'P' tipo, concat(store, replace(ddate,'-',''), reg, lpad(userno,3,'0')) id, ttime, totalamount s1, 0 s2 from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'F' and actioncode ='11' and recordcode1 = 0 and totalamount<>0
                        union
                        select 'H' tipo, concat(store, replace(ddate,'-','')) id, max(ttime) codice, 0 s1, 0 s2 from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'H' group by 1,2
                        union
                        select 'S' tipo, concat(e.store, replace(e.ddate,'-','')) id, '', e.status s1, e.eod s2 from `$tableNameEod` as e  where e.ddate = :ddate and e.store = :store
                        union
                        select 'I' tipo, concat(i.store, replace(i.ddate,'-',''), i.reg) id, i.reg codice, count(*) s1 , sum(i.`totalamount`) s2 from `$tableName` as i join (select store, ddate, reg, trans, userno from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'H' and recordcode1 = '1') as t on i.store = t.store and i.ddate=t.ddate and i.reg=t.reg and i.trans=t.trans
                        where binary i.recordtype = 'F' group by 1,2,3
                        union
                        select 'Q' tipo, concat(i.store, replace(i.ddate,'-',''), i.reg) id, i.reg codice, 0 s1 , 0 s2 from `$tableName` as i join (select store, ddate, reg, trans, userno from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'F' and recordcode1 <> '1') as t on i.store = t.store and i.ddate=t.ddate and i.reg=t.reg and i.trans=t.trans
                        where binary i.recordtype = 'F' and i.actioncode='99'
                        union
                        select 'R' tipo, concat(i.store, replace(i.ddate,'-','')) id, ifnull(cast(d.`printedCode` as int),1) codice, sum(case when i.totaltaxableamount *  i.totalamount < 0 then i.totaltaxableamount * -1 else i.totaltaxableamount end) s1, 1 s2 
                        from mtx.idc as i left join mtx.dep as d on lpad(i.userno,4,'0') = d.`subcode`  
                        where i.ddate = :ddate and i.store = :store and binary i.recordtype = 'S' and i.recordcode1 = 1 group by 1,2,3 order by 2,1,3
                        /*select 'R' tipo, concat(i.store, replace(i.ddate,'-','')) id, case when i.userno = 280 then 9 else case when i.userno < 10 then i.userno else case when i.userno >= 10 and i.userno < 100 then 1 else floor(i.userno/100) end end end codice, sum(case when i.quantita >= 0 then i.totaltaxableamount else i.totaltaxableamount * -1 end) s1, 0 s2 from `$tableName` as i join (select store, ddate, reg, trans from `$tableName` where ddate = :ddate and store = :store and binary recordtype = 'F' and recordcode1 = '1') as t on i.store = t.store and i.ddate=t.ddate and i.reg=t.reg and i.trans=t.trans where binary i.recordtype = 'S' group by 1,2,3
                        order by 2,1,3*/
                        ;";
            $handler = $this->pdo->prepare($stmt);

            $result = "";
            if ($handler->execute([
                ':ddate' => $request['data'],
                ':store' => $request['sede']
            ])) {
                $result = $handler->fetchAll( \PDO::FETCH_ASSOC );
            }

            return json_encode($result, true);

        } catch (PDOException $e) {
            return '';
        }
    }

    public function incassoPeriodo(array $request): string {
        try {
            $stmt = '';
            if (key_exists( 'dataFine', $request )) {
                $stmt .= "and ddate >= '" . $request['dataInizio'] . "'\n";
                $stmt .= "and ddate <= '" . $request['dataFine'] . "'\n";
            } elseif (key_exists( 'dataInizio', $request )) {
                $stmt .= "and ddate = '" . $request['dataInizio'] . "'\n";
            }
            if (key_exists( 'negozi', $request )) {
                if (count($request['negozi']) == 1) {
                    $stmt .= "and store = '" . $request['negozi'][0] . "'\n";
                } elseif (count($request['negozi']) > 1) {
                    $stmt .= "and store in ('" . implode("','", $request['negozi']). "')\n";
                }
            }
            if (key_exists( 'fasciaOraria', $request ) && $request['fasciaOraria']) {
                $stmt = "   select store, ddate, hour, sum(totalamount) totalamount, count(*) transactioncount 
                            from `".$this->schema . "`.`" . $this->tableName. "`
                            where binary recordtype = 'F' and recordcode1 = 1\n" . $stmt;
                $stmt .= "group by 1,2,3 order by 1,2;";
            } else {
                $stmt = "   select store, ddate, sum(totalamount) totalamount, count(*) transactioncount 
                            from `".$this->schema . "`.`" . $this->tableName. "`
                            where binary recordtype = 'F' and recordcode1 = 1\n" . $stmt;
                $stmt .= "group by 1,2 order by 1;";
            }
            $handler = $this->pdo->prepare($stmt);

            $result = [];
            if ($handler->execute()) {
                $result = $handler->fetchAll( \PDO::FETCH_ASSOC );
            }
            return json_encode( $result );

        } catch (PDOException $e) {
            return '';
        }
    }

    public function creazioneDatacollect(array $request): string {
        try {
            if (key_exists( 'data', $request ) && key_exists( 'sede', $request )) {
            	if (key_exists('cassa', $request) && key_exists('transazione', $request)) {
		            $stmt = "   select *  
	                            from `" . $this->schema . "`.`" . $this->tableName . "`
	                            where store = '" . $request['sede'] . "' and ddate = '" . $request['data'] . "' and 
			                          reg = '" . $request['cassa'] . "' and trans = '" . $request['transazione'] . "'
	                            order by ddate, reg, sequencenumber";
	            } else {
		            $stmt = "   select *  
	                            from `" . $this->schema . "`.`" . $this->tableName . "`
	                            where store = '" . $request['sede'] . "' and ddate = '" . $request['data'] . "'
	                            order by ddate, reg, sequencenumber";
	            }
                $handler = $this->pdo->prepare($stmt);

                $dc = [];
                $dc_v = [];
                if ($handler->execute()) {
                    $result = $handler->fetchAll( \PDO::FETCH_ASSOC );
                    foreach ($result as $row) {
                        if (preg_match('/^V$/',$row['recordtype']) && count($dc_v) ) {
                            array_splice( $dc, count($dc), 0, $dc_v );
                            $dc_v = [];
                        }
                        $mixedField = sprintf('%04d:%s%s', $row['userno'], $row['misc'], $row['data']);
                        if (preg_match('/^z$/',$row['recordtype'])) {
                            $mixedField = sprintf('%04d:%s', '0001',  substr($row['misc'] . $row['data'],3) . '000');
                        }
                        if (preg_match('/^m$/',$row['recordtype'])) {
                            $mixedField = sprintf('%s',  '  ' . $row['misc'] . $row['data'] . '   ');
                            if (preg_match('/:0492/', $row['misc'])) {
                                $mixedField = sprintf('%s',  '00' . $row['misc'] . $row['data'] . '   ');
                            }
                        }
                        $dc[] = sprintf('%04s:%03d:%06s:%06s:%04d:%03d:%1s:%03s:%s',
                            $row['store'],
                            $row['reg'],
                            substr(str_replace('-','', $row['ddate']),2),
                            $row['ttime'],
                            $row['trans'],
                            '000',
                            $row['recordtype'],
                            $row['recordcode1'].$row['recordcode2'].$row['recordcode3'],
                            $mixedField
                        );
                        if (preg_match('/^V$/',$row['recordtype'])) {
                            if ($row['totalamount'] <> 0 || $row['taxamount'] <> 0) {
                                $mixedField = sprintf( '%s%+010d', substr( $mixedField, 0, 30 ), round($row['taxamount'] * 100,0) );
                                $dc[] = sprintf( '%04s:%03d:%06s:%06s:%04d:%03d:%1s:%03s:%s',
                                    $row['store'],
                                    $row['reg'],
                                    substr( str_replace( '-', '', $row['ddate'] ), 2 ),
                                    $row['ttime'],
                                    $row['trans'],
                                    '000',
                                    $row['recordtype'],
                                    $row['recordcode1'] . $row['recordcode2'] . '0',
                                    $mixedField
                                );
                            }
                        }

                        if (preg_match('/^S$/',$row['recordtype'])) {
                            $dc[] = sprintf('%04s:%03d:%06s:%06s:%04d:%03d:%1s:%03s:%04d:%s:%011d%s',
                                    $row['store'],
                                    $row['reg'],
                                    substr(str_replace('-','', $row['ddate']),2),
                                    $row['ttime'],
                                    $row['trans'],
                                    '000',
                                    'i',
                                    $row['recordcode1'].'00',
                                    '0001',
                                    $row['misc'],
                                    $row['taxcode'],
                                    '3000000'
                            );
                            $dc[] = sprintf('%04s:%03d:%06s:%06s:%04d:%03d:%1s:%03s:%04d:%s:%04d%014d',
                                $row['store'],
                                $row['reg'],
                                substr(str_replace('-','', $row['ddate']),2),
                                $row['ttime'],
                                $row['trans'],
                                '000',
                                'i',
                                $row['recordcode1'].'01',
                                '0001',
                                $row['misc'],
                                $row['saleid'],
                                ''
                            );
                            /*if ( $row['store'] == '0101' &&  $row['trans'] == '3575' && preg_match('/80634492/',$row['misc']) ) {
                                echo "\n";
                            }*/
                            if (preg_match('/^1$/',$row['recordcode1'])) { //&& ($row['totaltaxableamount'] * 100) <> 0.00
                                if (preg_match('/^.{5}\./',$row['data'])) {
                                    $quantita = '0001';
                                } else {
                                    $quantita = sprintf('%04s', round($row['quantita'] ,0));
                                }
                                $dc_v[] = sprintf( '%04s:%03d:%06s:%06s:%04d:%03d:%1s:%03s:%04d:%s%+05d%07d%07d',
                                    $row['store'],
                                    $row['reg'],
                                    substr( str_replace( '-', '', $row['ddate'] ), 2 ),
                                    $row['ttime'],
                                    $row['trans'],
                                    '000',
                                    'v',
                                    $row['recordcode1'] . '00',
                                    '0001',
                                    $row['misc'],
                                    $quantita,
                                    round( $row['totaltaxableamount'] * 100, 0 ),
                                    round( $row['taxamount'] * 100, 0 )
                                );
                                $dc_v[] = sprintf( '%04s:%03d:%06s:%06s:%04d:%03d:%1s:%03s:%04d:%s:%04d%014d',
                                    $row['store'],
                                    $row['reg'],
                                    substr( str_replace( '-', '', $row['ddate'] ), 2 ),
                                    $row['ttime'],
                                    $row['trans'],
                                    '000',
                                    'v',
                                    $row['recordcode1'] . '01',
                                    '0001',
                                    $row['misc'],
                                    $row['saleid'],
                                    ''
                                );
                            }
                        }
                    }
                    $transStep = 1;
                    for($i=0;$i<count($dc);$i++) {
                        if ( preg_match('/^(.{28}).{3}.(.)(.{45})$/', $dc[$i], $matches)) {
                            $recordType = $matches[2];
                            $dc[$i] =  sprintf('%s%03d:%s%s', $matches[1], $transStep++, $matches[2], $matches[3]);
                            If (preg_match('/^F$/', $recordType)) {
                                $transStep = 1;
                            }
                        }
                    }
                    return implode("\r\n", $dc) . "\r\n";
                }
                return '';
            } else {
                return '';
            }

        } catch (PDOException $e) {
            return '';
        }
    }

    public function recuperaBarcode(array $request): array {
        $elencoArticoli = '(\'' . implode('\',\'', $request) . '\')';
        try {
            $stmt = "select b.`CODCIN-BAR2`, b.`BAR13-BAR2` from archivi.barartx2 as b where b.`CODCIN-BAR2` in $elencoArticoli";
            $handler = $this->pdo->prepare($stmt);

            $result = [];
            if ($handler->execute()) {
                foreach ($handler->fetchAll( \PDO::FETCH_ASSOC) as $row) {
                    if (key_exists($row['CODCIN-BAR2'], $result)) {
                        if (strlen($result[$row['CODCIN-BAR2']]) < strlen($row['BAR13-BAR2'])) {
                            $result[$row['CODCIN-BAR2']] = $row['BAR13-BAR2'];
                        }
                    } else {
                        $result[$row['CODCIN-BAR2']] = $row['BAR13-BAR2'];
                    }
                }
            }

            return $result;

        } catch (PDOException $e) {
            return [];
        }
    }

    public function recuperaReparto(array $request): array {
        $elencoArticoli = '(\'' . implode('\',\'', $request) . '\')';
        try {
            $stmt = "select CODICE_ARTICOLO codice, IDSOTTOREPARTO reparto from dimensioni.articolo where CODICE_ARTICOLO in $elencoArticoli";
            $handler = $this->pdo->prepare($stmt);

            $result = [];
            if ($handler->execute()) {
                foreach ($handler->fetchAll( \PDO::FETCH_ASSOC) as $row) {
                    $result[$row['codice']] = $row['reparto'];
                }
            }

            return $result;

        } catch (PDOException $e) {
            return [];
        }
    }

	public function recuperaCodiceArticoliPeso(): array {
		try {
			$stmt = "select CODICE_ARTICOLO codice from dimensioni.articolo where UM = 'KG' order by 1";
			$handler = $this->pdo->prepare($stmt);

			$result = [];
			if ($handler->execute()) {
				foreach ($handler->fetchAll( \PDO::FETCH_ASSOC) as $row) {
					$result[$row['codice']] = 'KG';
				}
			}

			return $result;

		} catch (PDOException $e) {
			return [];
		}
	}

	public function elencoTransazioniNonChiuse(array $request): array {
		try {
			$stmt = "
				select store, ddate, reg, trans, sum(case when recordtype = 'F' then 1 else 0 end) recCountF, count(*) recCount 
				from mtx.idc where ddate = :ddate and recordcode1 = 1 and binary recordtype in ('H','F') 
				group by 1,2,3,4 
				having recCount <> 2;";

			$handler = $this->pdo->prepare($stmt);

			$result = [];
			if ($handler->execute([':ddate' => $request['ddate']])) {
				$result = $handler->fetchAll( \PDO::FETCH_ASSOC );
			}
			return $result;

		} catch (PDOException $e) {
			return '';
		}
	}

	public function creazioneTestateScontrinoMancanti(array $request): array {
		try {
			$stmt = "
				select sequencenumber 
				from mtx.idc 
				where store = :store and ddate = :ddate and reg = :reg and trans = :trans 
				order by sequencenumber limit 1";
			$h_first_transaction_sequencenumber = $this->pdo->prepare($stmt);

			$stmt = "
				select sequencenumber 
				from mtx.idc 
				where store = :store and ddate = :ddate and sequencenumber < :sequencenumber 
				order by sequencenumber desc limit 1";
			$h_last_used_sequencenumber = $this->pdo->prepare($stmt);

			$stmt = "
				insert into mtx.idc
				select reg, store, ddate, ttime, `hour`, :sequencenumber , trans, transstep - 1 , 'H', '1', '0', '0', 1, '                ', ':00+00000+000000000', 0, 0, 0.00, 0.00, 0.00, 0.00, '', 0.000, 0, '', '', created_at 
				from mtx.idc 
				where store = :store and ddate = :ddate and reg = :reg and trans = :trans 
				order by sequencenumber
				limit 1;";
			$h_create_record = $this->pdo->prepare($stmt);

			$result = [];
			foreach ($request as $transaction) {
				if ($transaction['recCountF'] == 1 && $transaction['recCount'] == 1) {
					$h_first_transaction_sequencenumber->execute([
						':ddate' => $transaction['ddate'],
						':store' => $transaction['store'],
						':reg' => $transaction['reg'],
						':trans' => $transaction['trans']
					]);
					$firstTransactionSequenceNumber = ($h_first_transaction_sequencenumber->fetchAll(\PDO::FETCH_COLUMN))[0] * 1;

					$h_last_used_sequencenumber->execute([
						':ddate' => $transaction['ddate'],
						':store' => $transaction['store'],
						':sequencenumber' => $firstTransactionSequenceNumber
					]);
					$lastUsedSequenceNumber = ($h_last_used_sequencenumber->fetchAll(\PDO::FETCH_COLUMN))[0] * 1;

					if ($firstTransactionSequenceNumber - $lastUsedSequenceNumber > 1) {
						$h_create_record->execute([
							':ddate' => $transaction['ddate'],
							':store' => $transaction['store'],
							':reg' => $transaction['reg'],
							':trans' => $transaction['trans'],
							':sequencenumber' => $firstTransactionSequenceNumber - 1
						]);
					}
				}
			}

			return $result;

		} catch (PDOException $e) {
			return '';
		}
	}

	public function dettaglioQuadratura(array $request): string {
		try {
			$stmt = "
				select i.store, i.ddate, i.reg, i.trans, i.ttime, t.recordcode2 type, f.totalamount 
				from mtx.idc as i join 
				    (select distinct store, ddate, reg, trans, recordcode2 from mtx.idc where recordcode1 = 0 and binary recordtype = 'T' limit 1) as t on i.store=t.store and i.ddate=t.ddate and i.reg=t.reg and i.trans=t.trans join 
				    (select distinct store, ddate, reg, trans, totalamount from mtx.idc where recordcode1 = 0 and binary recordtype = 'F') as f on i.store=f.store and i.ddate=f.ddate and i.reg=f.reg and i.trans=f.trans 
				where i.store = :store and i.ddate = :ddate and i.recordcode1 = 0 and binary i.recordtype = 'H';";
			$h_query = $this->pdo->prepare($stmt);

			$result = '';


			return $result;

		} catch (PDOException $e) {
			return '';
		}
	}

	public function aggiornaStatoQuadratura(array $request): array {
		try {

			$stmt = "update mtx.eod set status = :status, eod = :eod where store = :store and ddate = :ddate";
			$h_query = $this->pdo->prepare($stmt);
			$h_query->execute([':store' => $request['store'], ':ddate' => $request['ddate'], ':status' => $request['status'], ':eod' => $request['eod']]);

			$stmt = "select status, eod from mtx.eod where store = :store and ddate = :ddate";
			$h_query = $this->pdo->prepare($stmt);
			$h_query->execute([':store' => $request['store'], ':ddate' => $request['ddate']]);
			$result = $h_query->fetchAll(\PDO::FETCH_ASSOC);

			$error = 100;
			if ($result[0]['status'] == $request['status'] && $result[0]['eod'] == $request['eod']) {
				$error = 0;
			}

			return ['error' => $error, 'status' => $result[0]['status'], 'eod' => $result[0]['eod']];

		} catch (PDOException $e) {
			return ['error' => 999]; //errore nell'aggiornamento db
		}
	}

	public function elencoTransazioni(array $request): array {
		try {
			$stmt = "
				select store, ddate, reg, trans, ttime, totalamount 
				from mtx.idc 
				where recordcode1 = 1 and binary recordtype = 'F' and store = :store and ddate = :ddate 
				order by 1,2,3,4";
			$h_query = $this->pdo->prepare($stmt);

			$h_query->execute([
				':store' => $request['store'],
				':ddate' => $request['ddate']
			]);
			$result = $h_query->fetchAll(\PDO::FETCH_ASSOC);

			return $result;

		} catch (PDOException $e) {
			return '';
		}
	}
}

