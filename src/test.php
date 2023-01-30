<?php

declare(strict_types=1);

@ini_set('memory_limit', '8192M');

use Database\Database;

$sqlDetails = [];
require(realpath(__DIR__ . '/..') . '/src/Database/autoload.php');

$db = new Database($sqlDetails);

$request = [
    'function' => 'recuperaDatiPerQuadratura',
    'data' => '2023-01-23',
    'sede' => '0134'
];

$result = $db->recuperaDatiPerQuadratura($request);

$rows = json_decode($result, true);

$filteredRows = [];
foreach ($rows as $row) {
    if ($row['tipo'] == 'F' && preg_match('/07\d$/', $row['id'])) {
        $filteredRows[] = $row;
    }
}
echo "\n";