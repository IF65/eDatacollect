<?php
    namespace Database;

    use Database\Viste\VMtxFatture;
    use \PDO;
    use Database\Tabelle\TIdc;
    use Database\Viste\VTcp_transazioni;
    use Database\Viste\VMtx_fatture;
    use PDOException;


    class Database {

        protected $pdo = null;
        private $db = [];
        
        public $t_idc = null;
        public $v_tcp_transazioni = null;
        public $v_mtx_fatture = null;

        private $sqlDetails = null;

        public function __construct(array $sqlDetails, $loadDb = True) {
            $this->sqlDetails = $sqlDetails;
            $this->loadDb = $loadDb;
            $conStr = sprintf("mysql:host=%s", $sqlDetails['host']);
            try {
                $this->pdo = new PDO($conStr, $sqlDetails['user'], $sqlDetails['password'], [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
                //$this->pdoMsSrv = new PDO("sqlsrv:Server=10.11.14.250,9089;Database=TCPOS4", "sa", "vfr456YHN");
                $this->db = $sqlDetails['db'];

                self::createDatabase();

            } catch (PDOException $e) {
                die($e->getMessage());
            }
        }

        public function createDatabase() {
        	try {
            
                // creazione schemi
                // ----------------------------------------------------------
                foreach($this->db as $key => $value) {
                    $stmt = $this->pdo->prepare("create database if not exists $value;");
                    $stmt->execute();
                }

                // creazione tabelle
                // ----------------------------------------------------------
                $this->t_idc = New TIdc($this->pdo, $this->db['mtx'], 'idc');


                // apertura viste
                // ----------------------------------------------------------
                $this->v_tcp_transazioni = New VTcp_transazioni();
		        $this->v_mtx_fatture = New VMtx_fatture($this->pdo);

                return true;
            } catch (PDOException $e) {
                die("DB ERROR: ". $e->getMessage());
            }
        }

        public function recuperaDatiContabili(array $request): string {
            return $this->t_idc->recuperaDatiContabili( $request );
        }

        public function recuperaDatiPerQuadratura(array $request): string {
            if ($request['sede'] == '0501' || $request['sede'] == '0201'|| $request['sede'] == '0155' || $request['sede'] == '0142' || $request['sede'] == '0203'|| $request['sede'] == '0204' || $request['sede'] == '0132'  || $request['sede'] == '0148' || $request['sede'] == '0115') {
	            $casseTCPOS = json_decode($this->v_tcp_transazioni->recuperaDatiPerQuadratura( $request ), true);
                $casseASAR = json_decode($this->t_idc->recuperaDatiPerQuadratura($request), true);

                $result = [];
                foreach ($casseTCPOS as $riga) {
                    if (preg_match('/^(0500)(.*)$/', $riga['id'], $matches)) {
                        $riga['id'] = '0501' . $matches[2];
                    }
	                if (preg_match('/^(\d{4}).*$/', $riga['id'], $matches)) {
	                	if ($matches[1] == $request['sede']) {
			                $result[] = $riga;
		                }
	                }
                }

                foreach ($casseASAR as $riga) {
                    $result[] = $riga;
                }

                return json_encode($result, true);

            } elseif ($request['sede'] == '0502' || $request['sede'] == '0503') {
                return $this->v_tcp_transazioni->recuperaDatiPerQuadratura( $request );

            } else {
                return $this->t_idc->recuperaDatiPerQuadratura($request);
                
            }

        }

        public function creazioneDatacollectTcPos(array $request): string {
            return $this->v_tcp_transazioni->creazioneDatacollectTcPos( $request );

        }

        public function incassoPeriodo(array $request): string {
            return $this->t_idc->incassoPeriodo($request);
        }

        public function creazioneDatacollect(array $request): string {
            return $this->t_idc->creazioneDatacollect($request);
        }

        public function creazioneDatacollectEpipoli(array $request): string {
            return $this->v_tcp_transazioni->creazioneDatacollectEpipoli($request);
        }

        public function creazioneDatacollectRiepvegi(array $request): string {
            $datacollectRiepvegi = json_decode($this->v_tcp_transazioni->creazioneDatacollectRiepvegi($request), true);

            $articoli = [];
            foreach($datacollectRiepvegi as $codice => $row) {
                $articoli[] = $codice;
            }

            $articoliBarcode = $this->t_idc->recuperaBarcode($articoli);
            foreach($articoliBarcode as $codice => $barcode) {
                $datacollectRiepvegi[$codice]['barcode'] = $barcode;
            }

            return json_encode($datacollectRiepvegi);
        }

        public function recuperaBarcode($request): string {
            return json_encode($this->t_idc->recuperaBarcode($request));

        }

        public function recuperaReparto($request): string {
            return json_encode($this->t_idc->recuperaReparto($request));

        }

	    public function recuperaCodiceArticoliPeso(): string {
		    return json_encode($this->t_idc->recuperaCodiceArticoliPeso());

	    }

        public function  creazioneDatacollectRiepvegiTxt(array $request): string {
            $dati = $this->creazioneDatacollectRiepvegi($request);
            return $this->v_tcp_transazioni->creazioneDatacollectRiepvegiTxt($request, $dati);
        }

        public function incassiInTempoReale(array $request): string {
            $rows = [];

	        $tcpos = $this->v_tcp_transazioni->incassiInTempoReale($request);
	        foreach($tcpos as $row) {
		        $index = $row['store'] . $row['ddate'];
		        $rows[$index] = [
			        'ddate' => $row['ddate'],
			        'store' => $row['store'],
			        'totalamount' => $row['totalamount'] * 1,
			        'customerCount' => $row['customerCount'] * 1
		        ];
				/*if ($row['store'] == '0500') {
					$index = '0501' . $row['ddate'];
					$rows[$index] = [
						'ddate' => $row['ddate'],
						'store' => '0501',
						'totalamount' => $row['totalamount'] * 1,
						'customerCount' => $row['customerCount'] * 1
					];
				}*/
	        }

	        $asar = $this->t_idc->incassiInTempoReale($request);
	        foreach($asar as $row) {
	        	$index = $row['store'] . $row['ddate'];

	        	if (key_exists($index, $rows)) {
			        $rows[$index]['totalamount'] += $row['totalamount'];
			        $rows[$index]['customerCount'] += $row['customerCount'];
		        } else {
			        $rows[$index] = [
				        'ddate' => $row['ddate'],
				        'store' => $row['store'],
				        'totalamount' => $row['totalamount'] * 1,
				        'customerCount' => $row['customerCount'] * 1
			        ];
		        }
	        }

	        return json_encode($rows);
        }

        public function recuperaMTXRigheFatture(array $request): string {
            return $this->t_idc->recuperaMTXRigheFatture( $request );
        }

	    public function dettaglioQuadratura(array $request): string {
		    return $this->t_idc->dettaglioQuadratura( $request );
	    }

	    public function aggiornaStatoQuadratura(array $request): array {
		    return $this->t_idc->aggiornaStatoQuadratura( $request );
	    }

        public function recuperaFatture(): string {
            return $this->v_tcp_transazioni->recuperaFatture();
        }

        public function creaFileInterscambioFatture(string $fatture): string {
            return $this->v_tcp_transazioni->creaFileInterscambioFatture($fatture);
        }

        public function elencoFattureEmesse(): string {
            return $this->v_tcp_transazioni->elencoFattureEmesse();
        }

        public function creaFatturaMtx(array $request):string {
	        return $this->v_mtx_fatture->creaFattura($request);
        }

        public function elencoTransazioni(array $request):array {
        	return $this->t_idc->elencoTransazioni($request);
        }

		public function creaJsonFattura(array $request): string {
			return $this->t_idc->creaJsonFattura($request);
		}

        public function __destruct() {
            $this->pdo = null;
        }
    }

