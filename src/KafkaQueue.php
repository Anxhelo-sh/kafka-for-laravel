<?php


namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Support\Facades\Log;

/**
 * @package Kafka
 */
class KafkaQueue extends Queue implements QueueContract
{
	/**
	 * @var \RdKafka\Producer
	 */
    protected \RdKafka\Producer $producer;

	/**
	 * @var \RdKafka\KafkaConsumer
	 */
    protected \RdKafka\KafkaConsumer $consumer;

    public function __construct(\RdKafka\Producer $producer, \RdKafka\KafkaConsumer $consumer)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;
    }

	/**
	 * @param $queue
	 * @return int
	 */
	public function size($queue = null)
	{
		return 0;
	}

	/**
	 * @param $job
	 * @param $data
	 * @param $queue
	 * @return true
	 */
	public function push($job, $data = '', $queue = null): true
	{
		$payload = $this->handleJobPayload($job, $data);

		return $this->pushRaw($payload, $queue);
	}

	/**
	 * @param $payload
	 * @param $queue
	 * @param array $options
	 * @return true
	 */
	public function pushRaw($payload, $queue = null, array $options = []): true
	{
		$topicName = $queue ?? env('KAFKA_QUEUE', 'default');

		$topic = $this->producer->newTopic($topicName);
		$topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload);
		$this->producer->flush(1000);

		return true;
	}

	/**
	 * @param $delay
	 * @param $job
	 * @param $data
	 * @param $queue
	 * @return mixed
	 */
	public function later($delay, $job, $data = '', $queue = null): mixed
	{
		// Kafka doesn't support delayed jobs natively, you can implement delayed topics or external scheduler
		throw new \BadMethodCallException('Delayed jobs are not supported by KafkaQueue.');
	}

	/**
	 * @param $queue
	 * @return void
	 */
	public function pop($queue = null): void
	{
		$queue = $queue ?? env('KAFKA_QUEUE', 'default');

		$this->consumer->subscribe([$queue]);

		try {
			$message = $this->consumer->consume(120 * 1000);

			switch ($message->err) {
				case RD_KAFKA_RESP_ERR_NO_ERROR:
					$job = $this->deserializePayload($message->payload);

					if ($job) {
						$job->handle();
					} else {
						Log::warning('KafkaQueue: Failed to unserialize job payload.');
					}
					break;

				case RD_KAFKA_RESP_ERR__PARTITION_EOF:
					// No more messages for now
					break;

				case RD_KAFKA_RESP_ERR__TIMED_OUT:
					// Timeout, no messages received
					break;

				default:
					throw new \Exception($message->errstr(), $message->err);
			}
		} catch (\Throwable $e) {
			Log::error('KafkaQueue error: ' . $e->getMessage());
		}
	}

	/**
	 * @param $job
	 * @param $data
	 * @return string
	 */
	protected function handleJobPayload($job, $data): string
	{
		// If $job is already a Job instance, serialize it; otherwise, create a JobPayload
		if (is_string($job)) {
			$job = $this->marshalJob($job, $data);
		}

		return serialize($job);
	}

	/**
	 * @param $payload
	 * @return mixed|null
	 */
	protected function deserializePayload($payload): mixed
	{
		try {
			return unserialize($payload);
		} catch (\Exception $e) {
			\Log::error('KafkaQueue deserialize error: ' . $e->getMessage());
			return null;
		}
	}

	/**
	 *  Create a Laravel job instance dynamically here if needed
	 *
	 * @param $job
	 * @param $data
	 * @return mixed
	 */
	protected function marshalJob($job, $data): mixed
	{
		return new $job($data);
	}
}
