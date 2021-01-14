<?php


namespace Database\Viste;


class VMtx_fatture
{
	protected $pdo = null;

	public function __construct($pdo) {
		$this->pdo = $pdo;
	}

	public function creaFattura(array $request): string {
		try {
			$sql = "select 
						ifnull(b.`CODCIN-BAR2`,'') articoloCodice,
						ifnull(a.`DES-ART2`,'') articoloDescrizione,
						i.barcode plu, 
						i.weightPlu pluPeso, 
						lpad(i.userno,4,'0') repartoCassa,
						i.quantita,
						i.taxcode ivaCodice, 
						i.totaltaxableamount importoTotale, 
						i.totaltaxableamount - i.taxamount imponibileTotale,
						i.taxamount impostaTotale
					from (select store, ddate, reg, trans, case when substr(barcode,9,4)='0000' then substr(barcode,1,7) else barcode end barcode, case when substr(barcode,9,4)='0000' then 1 else 0 end weightPlu, userno, quantita, taxcode, totaltaxableamount, taxamount from mtx.idc where binary recordtype = 'S' and recordcode1 = 1) as i left join archivi.barartx2 as b on i.barcode = b.`BAR13-BAR2` left join archivi.articox2 as a on b.`CODCIN-BAR2`=a.`COD-ART2` 
					where i.store = '0101' and i.ddate = '21-01-13' and i.reg = '002' and i.trans = '0344' and i.totaltaxableamount <> 0";
			$stmt = $this->pdo->prepare($sql);
			$stmt->execute([
				'store' => $request['store'],
				'ddate' => $request['ddate'],
				'reg' => $request['reg'],
				'trans' => $request['trans'],
			]);
			$result = $stmt->fetchAll( \PDO::FETCH_ASSOC );

            return json_encode(['errorMessage' => '', 'rows' => $result, 'status'=>0]);
		} catch (PDOException $e) {
			json_encode(['errorMessage' => $e->getMessage(), 'rows' => '', 'status'=>100]);
		}
	}
}