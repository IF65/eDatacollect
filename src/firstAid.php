<?php
@ini_set('memory_limit','8192M');

$debug = true;

$sqlDetails = [];

require(realpath(__DIR__ . '/..').'/src/Database/autoload.php');

use Database\Database;

$timeZone = new \DateTimeZone('Europe/Rome');

$db = new Database($sqlDetails);

$result = $db->t_idc->elencoTransazioniNonChiuse(['ddate' => '2021-01-10']);
$db->t_idc->creazioneTestateScontrinoMancanti($result);
print_r($result);