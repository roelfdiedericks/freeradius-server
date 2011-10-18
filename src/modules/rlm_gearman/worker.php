#!/usr/bin/php -n
<?

dl("gearman.so");

define('RLM_MODULE_REJECT',0);		/* immediately reject the request */
define('RLM_MODULE_FAIL',1);		/* module failed, don't reply */
define('RLM_MODULE_OK',2);			/* the module is OK, continue */
define('RLM_MODULE_HANDLED',3);		/* the module handled the request, so stop. */
define('RLM_MODULE_INVALID',4); 	/* the module considers the request invalid. */
define('RLM_MODULE_USERLOCK',5);	/* reject the request (user is locked out) */
define('RLM_MODULE_NOTFOUND',6);	/* user not found */
define('RLM_MODULE_NOOP','7');		/* module succeeded without doing anything */
define('RLM_MODULE_UPDATED',8);		/* OK (pairs modified) */
define('RLM_MODULE_NUMCODES',9);	/* How many return codes there are */


//  Create our worker object.
$gmworker= new GearmanWorker();
// Add default server (localhost).
echo("registering with gearman\n");
$gmworker->addServer('127.0.0.1');
$gmworker->addFunction("gear_authenticate", "radius");
$gmworker->addFunction("gear_authorize", "radius");
$gmworker->addFunction("gear_postauth", "radius");
$gmworker->addFunction("gear_preacct", "radius");
$gmworker->addFunction("gear_accounting", "radius");
$gmworker->addFunction("gear_checksimul", "radius");
$gmworker->addFunction("gear_pre_proxy", "radius");
$gmworker->addFunction("gear_post_proxy", "radius");
$gmworker->addFunction("gear_recv_coa", "radius");
$gmworker->addFunction("gear_send_coa", "radius");
echo "worker started\n";
while($gmworker->work())
{
    if ($gmworker->returnCode() != GEARMAN_SUCCESS)
    {
        echo "return_code: " . $gmworker->returnCode() . "\n";
        break;
    }
}

function radius($job) {
	$data=json_decode($job->workload(),true);
	echo "received job" . date("r") . " {$data['type']}\n";
	print_r($data);

	if (1) {
		echo "allowing user {$data['request']['User-Name']}\n";
		if (!empty($data['request']['User-Password'])) {
			$data['control']['Cleartext-Password']=$data['request']['User-Password'];
		}
		$data['statuscode']=RLM_MODULE_OK;
	} else {
		$data['statuscode']=RLM_MODULE_REJECT;
	}
	$data['reply']['Session-Timeout']=7200;
    echo "Sending response\n";
	//print_r($data);
	$response=json_encode($data)."\0";
    return $response;
}
