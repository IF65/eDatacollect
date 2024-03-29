<?php
@ini_set('memory_limit', '8192M');

$debug = false;

$sqlDetails = [];

require(realpath(__DIR__ . '/..') . '/src/Database/autoload.php');

use Database\Database;

$timeZone = new \DateTimeZone('Europe/Rome');

if ($debug) {
	//$input = "{\"function\":\"creaFatturaMtx\",\"ddate\":\"2021-12-21\",\"store\":\"0102\",\"reg\":\"0102\",\"trans\":\"8125\"}";
	//$input = "{\"function\":\"recuperaMTXRigheFatture\",\"ddate\":\"2021-01-03\",\"store\":\"0101\",\"reg\":\"001\",\"trans\":\"6228\"}";
	//$input = "{\"function\":\"recuperaDatiPerQuadratura\",\"data\":\"2022-07-26\",\"sede\":\"0501\"}";
	//$input = "{\"function\":\"recuperaDatiPerQuadratura\",\"data\":\"2023-01-23\",\"sede\":\"0134\"}";
	//$input = "{\"function\":\"creazioneDatacollect\",\"data\":\"2022-06-28\",\"sede\":\"0134\",\"cassa\":\"001\",\"transazione\":\"1561\"}";
	//$input = "{\"function\":\"creazioneDatacollect\",\"data\":\"2022-11-03\",\"sede\":\"0136\"}";
	$input = "{\"function\":\"recuperaFatture\"}";
	//$input = "{\"function\":\"creaFileInterscambioFatture\"}";
	//$input = "{\"function\":\"creazioneDatacollectRiepvegiTxt\",\"data\":\"2020-09-06\",\"sede\":\"0501\"}";
	//$fatture = file_get_contents('/Users/if65/Desktop/fatture.json');
	//file_put_contents('/Users/if65/Desktop/testDC.txt', implode("\n",json_decode($result, true)));
	//file_put_contents('/Users/if65/Desktop/testFatture.txt', $result);
	//$input = "{\"function\":\"recuperaBarcode\",\"articoli\":[\"6364070\"]}";
	//$input = "{\"function\":\"recuperaSospesi\",\"data\":\"2021-03-24\",\"sede\":\"0201\"}";
	//$input = "{\"function\":\"elencoTransazioni\",\"ddate\":\"2021-07-06\",\"store\":\"0101\"}";

	//$input = "{\"function\":\"aggiornaStatoQuadratura\",\"ddate\":\"2021-02-22\",\"store\":\"0155\",\"status\":\"3\",\"eod\":\"1\"}";
	//$input = '{"function":"aggiornaStatoQuadratura","store":"0101","ddate":"2021-07-22T00:00:00","status":0,"eod":0}';
	//$input = "{\"function\":\"incassiInTempoRealeTxt\",\"data\":\"2021-06-23\"}";
	//$input = '{"function":"recuperaCodiceArticoliPeso"}';
	//$input= file_get_contents("/Users/if65/Desktop/request.json");

	$request = json_decode($input, true);
} else {
	$input = file_get_contents('php://input');
	$request = json_decode($input, true);
}

if (!isset($request)) {
	die;
}

$db = new Database($sqlDetails);

if ($request['function'] == 'getDatiContabili') {
	$result = $db->recuperaDatiContabili($request);
	echo $result;

} elseif ($request['function'] == 'recuperaDatiPerQuadratura') {
	$result = $db->recuperaDatiPerQuadratura($request);
	echo $result;

} elseif ($request['function'] == 'creazioneDatacollectTcPos') {
	$result = $db->creazioneDatacollectTcPos($request);
	echo $result;

} elseif ($request['function'] == 'incassoPeriodo') {
	$result = $db->incassoPeriodo($request);
	echo $result;

} elseif ($request['function'] == 'creazioneDatacollect') {
	$result = $db->creazioneDatacollect($request);
	if ($debug) {
		if (preg_match('/^(\d{2})(\d{2})-(\d{2})-(\d{2})/', $request['data'], $m)) {
			$fileName = $request['sede'] . '_' . $m[1] . $m[2] . $m[3] . $m[4] . '_' . $m[2] . $m[3] . $m[4] . '_DC.TXT';
			file_put_contents("/Users/if65/Desktop/$fileName", $result);
		}
	}
	echo $result;

} elseif ($request['function'] == 'creazioneDatacollectEpipoli') {
	$result = $db->creazioneDatacollectEpipoli($request);
	file_put_contents("/Users/if65/Desktop/DC.TXT", $result);
	echo $result;

} elseif ($request['function'] == 'creazioneDatacollectRiepvegi') {
	$result = $db->creazioneDatacollectRiepvegi($request);
	echo $result;

} elseif ($request['function'] == 'creazioneDatacollectRiepvegiTxt') {
	$result = $db->creazioneDatacollectRiepvegiTxt($request);
	echo $result;

} elseif ($request['function'] == 'incassiInTempoReale') {
	$result = $db->incassiInTempoReale($request);
	echo $result;

} elseif ($request['function'] == 'incassiInTempoRealeTxt') {
	$incassiJson = $db->incassiInTempoReale($request);

	$incassi = json_decode($incassiJson, true);

	$result = '';
	foreach ($incassi as $incasso) {
		$result .= sprintf('%s%s%+012.2f%08d', str_replace('-', '', $incasso['ddate']), $incasso['store'], $incasso['totalamount'], $incasso['customerCount']) . "\n";
	}
	echo $result;

} elseif ($request['function'] == 'recuperaMTXRigheFatture') {
	$result = $db->recuperaMTXRigheFatture($request);
	echo $result;

} elseif ($request['function'] == 'recuperaFatture') {
	$result = $db->recuperaFatture();
	echo $result;

} elseif ($request['function'] == 'creaFileInterscambioFatture') {
	$fatture = $db->recuperaFatture();
	$result = $db->creaFileInterscambioFatture($fatture);
	echo $result;

} elseif ($request['function'] == 'elencoFattureEmesse') {
	$result = $db->elencoFattureEmesse();
	echo $result;

} elseif ($request['function'] == 'recuperaBarcode') {
	$result = $db->recuperaBarcode($request['articoli']);
	echo $result;

} elseif ($request['function'] == 'recuperaReparto') {
	$result = $db->recuperaReparto($request['articoli']);
	echo $result;

} elseif ($request['function'] == 'recuperaCodiceArticoliPeso') {
	$result = $db->recuperaCodiceArticoliPeso();
	echo $result;

} elseif ($request['function'] == 'recuperaSospesi') {
	$host = '10.11.14.85';
	$user = 'if65';
	$password = 'uN7veDu7h2vg';
	$dbName = 'eFatture';

	//CALL eFatture.get_sospesi_giorno(‘YYYY-MM-DD’)
	//CALL get_fatture_giorno('2020-10-31', '0132');
	try {
		$db = new PDO("mysql:host=$host;dbname=$dbName", $user, $password);
		// execute the stored procedure
		$stmt = 'CALL get_fatture_giorno(:data, :sede);';

		$h_query = $db->prepare($stmt);
		$h_query->execute([':data' => $request['data'], ':sede' => $request['sede']]);
		$result = $h_query->fetchAll(PDO::FETCH_ASSOC);
		echo json_encode($result);

	} catch (PDOException $e) {
		$result = [];
		echo json_encode($result);
	}
} elseif ($request['function'] == 'creaFatturaMtx') {
	$result = $db->creaFatturaMtx($request);
	echo $result;

} elseif ($request['function'] == 'dettaglioQuadratura') {
	$result = json_encode($db->dettaglioQuadratura($request));

	echo $result;

} elseif ($request['function'] == 'aggiornaStatoQuadratura') {
	$result = json_encode($db->aggiornaStatoQuadratura($request));

	echo $result;

} elseif ($request['function'] == 'elencoTransazioni') {
	$result = json_encode($db->elencoTransazioni($request));

	echo $result;

}