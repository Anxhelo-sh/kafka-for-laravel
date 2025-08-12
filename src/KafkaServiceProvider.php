<?php

namespace Kafka;

use Illuminate\Support\ServiceProvider;

/**
 * @package Kafka
 */
class KafkaServiceProvider extends ServiceProvider
{
	/**
	 * @return void
	 */
    public function boot(): void
    {
        $manager = $this->app['queue'];

        $manager->addConnector('kafka', function () {
            return new KafkaConnector;
        });
    }
}
