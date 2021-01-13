<?php
@ini_set('memory_limit','8192M');

$debug = true;

$sqlDetails = [];

require(realpath(__DIR__ . '/..').'/src/Database/autoload.php');

use Database\Database;
$db = new Database($sqlDetails);

$timeZone = new \DateTimeZone('Europe/Rome');


$result = $db->creazioneDatacollectEpipoli(['sede' => '0501', 'data' => '2020-08-12']);
file_put_contents('/Users/if65/Desktop/test/epipoli.txt', $result);
/*$transazioni = json_decode(file_get_contents('/Users/if65/Desktop/test/tcpos_20200810.json'), true);

foreach ($transazioni as $id => $transazione) {
    $dc = [];

    $dc[$id] = $transazione;

    $totaleVenduto = 0;

    $riepvegi = $db->v_tcp_transazioni->testRiepvegi(['data' => '2020-08-10', 'sede' => '0501'], json_encode($dc));
    foreach ( json_decode($riepvegi, true) as $articoli) {
        $totaleVenduto += $articoli['venduto'];
    }
    echo "$id\t$totaleVenduto\t".$transazione['total_amount']."\n";
}*/



