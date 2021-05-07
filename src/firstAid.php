<?php
@ini_set('memory_limit','8192M');

$debug = true;

$sqlDetails = [];

require(__DIR__ . '/..');

use Database\Database;

$timeZone = new \DateTimeZone('Europe/Rome');

$db = new Database($sqlDetails);

$currentDate = new DateTime();

//$date = $currentDate->sub(new DateInterval('P1D'))->format('Y-m-d');
$date = $currentDate->format('Y-m-d');

$result = $db->t_idc->elencoTransazioniNonChiuse(['ddate' => $date]);
$db->t_idc->creazioneTestateScontrinoMancanti($result);
print_r($result);