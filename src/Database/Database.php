<?php

namespace Database;

use Database\Viste\VMtxFatture;
use \PDO;
use Database\Tabelle\TIdc;
use Database\Viste\VTcp_transazioni;
use Database\Viste\VMtx_fatture;
use PDOException;


class Database
{

    protected $pdo = null;
    private $db = [];

    public $t_idc = null;
    public $v_tcp_transazioni = null;
    public $v_mtx_fatture = null;

    private $sqlDetails = null;

    public function __construct(array $sqlDetails, $loadDb = true)
    {
        $this->sqlDetails = $sqlDetails;
        $this->loadDb = $loadDb;
        $conStr = sprintf("mysql:host=%s", $sqlDetails['host']);
        try {
            $this->pdo = new PDO(
                $conStr,
                $sqlDetails['user'],
                $sqlDetails['password'],
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
            );
            //$this->pdoMsSrv = new PDO("sqlsrv:Server=10.11.14.250,9089;Database=TCPOS4", "sa", "vfr456YHN");
            $this->db = $sqlDetails['db'];

            self::createDatabase();
        } catch (PDOException $e) {
            die($e->getMessage());
        }
    }

    public function createDatabase()
    {
        try {
            // creazione schemi
            // ----------------------------------------------------------
            foreach ($this->db as $key => $value) {
                $stmt = $this->pdo->prepare("create database if not exists $value;");
                $stmt->execute();
            }

            // creazione tabelle
            // ----------------------------------------------------------
            $this->t_idc = new TIdc($this->pdo, $this->db['mtx'], 'idc');


            // apertura viste
            // ----------------------------------------------------------
            $this->v_tcp_transazioni = new VTcp_transazioni();
            $this->v_mtx_fatture = new VMtx_fatture($this->pdo);

            return true;
        } catch (PDOException $e) {
            die("DB ERROR: " . $e->getMessage());
        }
    }

    public function recuperaDatiContabili(array $request): string
    {
        return $this->t_idc->recuperaDatiContabili($request);
    }

    public function recuperaDatiPerQuadratura(array $request): string
    {
        if ($request['sede'] == '0501' || $request['sede'] == '0201' || $request['sede'] == '0155' || $request['sede'] == '0142' ||
            $request['sede'] == '0203' || $request['sede'] == '0204' || $request['sede'] == '0132' || $request['sede'] == '0148' ||
            $request['sede'] == '0115' || $request['sede'] == '0101' || $request['sede'] == '0205' || $request['sede'] == '0104' ||
            $request['sede'] == '0133' || $request['sede'] == '3661' || $request['sede'] == '0125' || $request['sede'] == '0139' ||
            $request['sede'] == '0108' || $request['sede'] == '0110' || $request['sede'] == '0173' || $request['sede'] == '3151' ||
            $request['sede'] == '3673' || $request['sede'] == '3694' || $request['sede'] == '0188' || $request['sede'] == '0178' ||
            $request['sede'] == '0190' || $request['sede'] == '0131' || $request['sede'] == '0129' || $request['sede'] == '3152' ||
            $request['sede'] == '0127' || $request['sede'] == '0119' || $request['sede'] == '0143' || $request['sede'] == '0124' ||
            $request['sede'] == '0141' || $request['sede'] == '0114' || $request['sede'] == '0177' || $request['sede'] == '3682' ||
            $request['sede'] == '0128' || $request['sede'] == '0202' || $request['sede'] == '0140' || $request['sede'] == '3693' ||
            $request['sede'] == '0172' || $request['sede'] == '0176'
        ) {
            $casseTCPOS = json_decode($this->v_tcp_transazioni->recuperaDatiPerQuadratura($request), true);
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
        } elseif ($request['sede'] == '0134') {
            $result = [];

            $casseASAR = json_decode($this->t_idc->recuperaDatiPerQuadratura($request), true);
            foreach ($casseASAR as $riga) {
                $result[] = $riga;
            }

            $request['sede'] = '7134';
            $casseCC = json_decode($this->t_idc->recuperaDatiPerQuadratura($request), true);
            foreach ($casseCC as $riga) {
                if (preg_match('/^(7134)(.*)$/', $riga['id'], $matches)) {
                    $riga['id'] = '0134' . $matches[2];
                }
                $result[] = $riga;
            }

            $request['sede'] = '8134';
            $casseHD = json_decode($this->t_idc->recuperaDatiPerQuadratura($request), true);
            foreach ($casseHD as $riga) {
                if (preg_match('/^(8134)(.*)$/', $riga['id'], $matches)) {
                    $riga['id'] = '0134' . $matches[2];
                }

                $found = false;
                foreach ($result as $k => $row) {
                    if ($row['tipo'] == $riga['tipo'] && $row['id'] == $riga['id']) {
                        $result[$k]['s1'] += $riga['s1'];
                        $result[$k]['s2'] += $riga['s2'];
                        $found = true;
                    }
                }

                if (!$found) {
                    $result[] = $riga;
                }
            }

            return json_encode($result, true);
        } elseif ($request['sede'] == '0502' || $request['sede'] == '0503') {
            return $this->v_tcp_transazioni->recuperaDatiPerQuadratura($request);
        } else {
            return $this->t_idc->recuperaDatiPerQuadratura($request);
        }
    }

    public function creazioneDatacollectTcPos(array $request): string
    {
        return $this->v_tcp_transazioni->creazioneDatacollectTcPos($request);
    }

    public function incassoPeriodo(array $request): string
    {
        return $this->t_idc->incassoPeriodo($request);
    }

    public function creazioneDatacollect(array $request): string
    {
        return $this->t_idc->creazioneDatacollect($request);
    }

    public function creazioneDatacollectEpipoli(array $request): string
    {
        return $this->v_tcp_transazioni->creazioneDatacollectEpipoli($request);
    }

    public function creazioneDatacollectRiepvegi(array $request): string
    {
        $datacollectRiepvegi = json_decode($this->v_tcp_transazioni->creazioneDatacollectRiepvegi($request), true);

        $articoli = [];
        foreach ($datacollectRiepvegi as $codice => $row) {
            $articoli[] = $codice;
        }

        $articoliBarcode = $this->t_idc->recuperaBarcode($articoli);
        foreach ($articoliBarcode as $codice => $barcode) {
            $datacollectRiepvegi[$codice]['barcode'] = $barcode;
        }

        return json_encode($datacollectRiepvegi);
    }

    public function recuperaBarcode($request): string
    {
        return json_encode($this->t_idc->recuperaBarcode($request));
    }

    public function recuperaReparto($request): string
    {
        return json_encode($this->t_idc->recuperaReparto($request));
    }

    public function recuperaCodiceArticoliPeso(): string
    {
        return json_encode($this->t_idc->recuperaCodiceArticoliPeso());
    }

    public function creazioneDatacollectRiepvegiTxt(array $request): string
    {
        $dati = $this->creazioneDatacollectRiepvegi($request);
        return $this->v_tcp_transazioni->creazioneDatacollectRiepvegiTxt($request, $dati);
    }

    public function incassiInTempoReale(array $request): string
    {
        /*$rows = [];

        $tcpos = $this->v_tcp_transazioni->incassiInTempoReale($request);
        foreach($tcpos as $row) {
            $index = $row['store'] . $row['ddate'];
            $rows[$index] = [
                'ddate' => $row['ddate'],
                'store' => $row['store'],
                'totalamount' => $row['totalamount'] * 1,
                'customerCount' => $row['customerCount'] * 1
            ];
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
        }*/

        $rows = $this->t_idc->incassiInTempoReale($request);

        return json_encode($rows);
    }

    public function recuperaMTXRigheFatture(array $request): string
    {
        return $this->t_idc->recuperaMTXRigheFatture($request);
    }

    public function dettaglioQuadratura(array $request): string
    {
        return $this->t_idc->dettaglioQuadratura($request);
    }

    public function aggiornaStatoQuadratura(array $request): array
    {
        return $this->t_idc->aggiornaStatoQuadratura($request);
    }

    public function recuperaFatture(): string
    {
        return $this->v_tcp_transazioni->recuperaFatture();
    }

    public function creaFileInterscambioFatture(string $fatture): string
    {
        return $this->v_tcp_transazioni->creaFileInterscambioFatture($fatture);
    }

    public function elencoFattureEmesse(): string
    {
        return $this->v_tcp_transazioni->elencoFattureEmesse();
    }

    public function creaFatturaMtx(array $request): string
    {
        return $this->v_mtx_fatture->creaFattura($request);
    }

    public function elencoTransazioni(array $request): array
    {
        return $this->t_idc->elencoTransazioni($request);
    }

    public function creaJsonFattura(array $request): string
    {
        return $this->t_idc->creaJsonFattura($request);
    }

    public function __destruct()
    {
        $this->pdo = null;
    }
}

