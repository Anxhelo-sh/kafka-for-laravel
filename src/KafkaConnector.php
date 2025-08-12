<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

/**
 * @package Kafka
 */
class KafkaConnector implements ConnectorInterface
{
	/**
	 * @param array $config
	 * @return KafkaQueue
	 */
    public function connect(array $config): KafkaQueue
    {
	    $conf = new \RdKafka\Conf();

	    $conf->set('bootstrap.servers', $config['bootstrap_servers']);
        $conf->set('security.protocol', $config['security_protocol']);
	    $conf->set('group.id', $config['group_id']);
	    $conf->set('auto.offset.reset', 'earliest');

	    if (!empty($config['sasl_mechanisms'])) {
		    $conf->set('sasl.mechanisms', $config['sasl_mechanisms']);
	    }
	    if (!empty($config['sasl_username'])) {
		    $conf->set('sasl.username', $config['sasl_username']);
	    }
	    if (!empty($config['sasl_password'])) {
		    $conf->set('sasl.password', $config['sasl_password']);
	    }

	    $producer = new \RdKafka\Producer($conf);
	    $consumer = new \RdKafka\KafkaConsumer($conf);

        return new KafkaQueue($producer, $consumer);
    }
}
