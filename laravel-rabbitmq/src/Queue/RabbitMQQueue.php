<?php

namespace Mustah\Queue;

use DateTime;
use GuzzleHttp\Client;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Mustah\Queue\Jobs\RabbitMQJob;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use RabbitMQ\Management\APIClient;

class RabbitMQQueue extends Queue implements QueueContract
{

	protected $connection;
	protected $channel;

	protected $declareExchange;
	protected $declareBindQueue;

	protected $defaultQueue;
	protected $configQueue;
	protected $configExchange;

    /**
     * @param AMQPStreamConnection $amqpConnection
     * @param array $config
     */


	/*
	 *
	 *  example $config = array(11) {

  								  ["driver"]=> string(8) "rabbitmq"
								  ["host"]=> string(8) "10.0.2.2"
								  ["port"]=> string(4) "5672"
								  ["vhost"]=> string(1) "/"
								  ["login"]=> string(5) "guest"
								  ["password"]=> string(5) "guest"
								  ["queue"]=> string(3) "cnc"
								  ["exchange_declare"]=> bool(true)
								  ["queue_declare_bind"]=> string(3) "cnc"

								  ["queue_params"]=> array(4) {
														["passive"]=> bool(false)
														["durable"]=> bool(true)
														["exclusive"]=> bool(false)
														["auto_delete"]=> bool(false)
								  						}

								  ["exchange_params"]=> array(5) {
														["name"]=> string(17) "messages.exchange"
														["type"]=> string(6) "direct"
														["passive"]=> bool(false)
														["durable"]=> bool(true)
														["auto_delete"]=> bool(false)
														  }
								}
	 */



    public function __construct(AMQPStreamConnection $amqpConnection, $config)
	{
		$this->connection = $amqpConnection;
		$this->defaultQueue = $config['queue'];
		$this->configQueue = $config['queue_params'];
		$this->configExchange = $config['exchange_params'];
		$this->declareExchange = $config['exchange_declare'];
		$this->declareBindQueue = $config['queue_declare_bind'];

		$this->channel = $this->getChannel();
	}

	/**
	 * Push a new job onto the queue.
	 *
	 * @param  string $job
	 * @param  mixed  $data
	 * @param  string $queue
	 *
	 * @return bool
	 */
	public function push($job, $data = '', $queue = null)
	{
		return $this->pushRaw($this->createPayload($job, $data), $queue, []);
	}

	/**
	 * Push a raw payload onto the queue.
	 *
	 * @param  string $payload
	 * @param  string $queue
	 * @param  array  $options
	 *
	 * @return mixed
	 */
	public function pushRaw($payload, $queue = null, array $options = [])
	{
		$queue = $this->getQueueName($queue);
		$exchange = $this->configExchange['name'] ?:$queue;
		$this->declareQueue($queue);
		if (isset($options['delay'])) {
			$queue = $this->declareDelayedQueue($queue, $options['delay']);
		}

		// Roy N
		if (isset($options['failed'])) {
			$queue = $this->declareFailedQueue($queue, $options['failed']);
		}

		// push job to a queue
		$message = new AMQPMessage($payload, [
			'Content-Type'  => 'application/json',
			'delivery_mode' => 2,
		]);

		// push task to a queue
		$this->channel->basic_publish($message, $exchange, $queue);

		return true;
	}

	/**
	 * Push a new job onto the queue after a delay.
	 *
	 * @param  \DateTime|int $delay
	 * @param  string        $job
	 * @param  mixed         $data
	 * @param  string        $queue
	 *
	 * @return mixed
	 */
	public function later($delay, $job, $data = '', $queue = null)
	{
		return $this->pushRaw($this->createPayload($job, $data), $queue, ['delay' => $delay]);
	}

	/**
	 * Pop the next job off of the queue.
	 *
	 * @param string|null $queue
	 *
	 * @return \Illuminate\Queue\Jobs\Job|null
	 */
	public function pop($queue = null)
	{
		$queue = $this->getQueueName($queue);

		// declare queue if not exists
		$this->declareQueue($queue);

		// get envelope
		$message = $this->channel->basic_get($queue);

		if ($message instanceof AMQPMessage) {
			return new RabbitMQJob($this->container, $this, $this->channel, $queue, $message);
		}

		return null;
	}

	/**
	 * @param string $queue
	 *
	 * @return string
	 */
	private function getQueueName($queue)
	{
		return $queue ?: $this->defaultQueue;
	}

	/**
	 * @return AMQPChannel
	 */
	private function getChannel()
	{
		return $this->connection->channel();
	}

	/**
	 * @param string $name
	 */
	private function declareQueue($name)
	{
		$name = $this->getQueueName($name);
		$exchange = $this->configExchange['name'] ?:$name;

		if ($this->declareExchange) {
			// declare exchange
			$this->channel->exchange_declare(
				$exchange,
				$this->configExchange['type'],
				$this->configExchange['passive'],
				$this->configExchange['durable'],
				$this->configExchange['auto_delete']
			);
		}

		if ($this->declareBindQueue) {
			// declare queue
			$this->channel->queue_declare(
				$name,
				$this->configQueue['passive'],
				$this->configQueue['durable'],
				$this->configQueue['exclusive'],
				$this->configQueue['auto_delete']
			);

			// bind queue to the exchange
			$this->channel->queue_bind($name, $exchange, $name);
		}
	}

	/**
	 * @param string       $destination
	 * @param DateTime|int $delay
	 *
	 * @return string
	 */
	private function declareDelayedQueue($destination, $delay)
	{

		$delay = $this->getSeconds($delay);
		$destination = $this->getQueueName($destination);
		$name = $this->getQueueName($destination) . '_deferred_' . $delay;
		$exchange = $this->configExchange['name'] ?:$name;

		// declare exchange
		$this->channel->exchange_declare(
            $exchange,
			$this->configExchange['type'],
			$this->configExchange['passive'],
			$this->configExchange['durable'],
			$this->configExchange['auto_delete']
		);

		// declare queue
		$this->channel->queue_declare(
            $name,
			$this->configQueue['passive'],
			$this->configQueue['durable'],
			$this->configQueue['exclusive'],
			$this->configQueue['auto_delete'],
			false,
			new AMQPTable([
				'x-dead-letter-exchange'    => $destination,
				'x-dead-letter-routing-key' => $destination,
				'x-message-ttl'             => $delay * 1000,
			])
		);

		// bind queue to the exchange
		$this->channel->queue_bind($name, $exchange, $name);

		return $name;
	}

	/**
	 * @param string       $destination
	 * @param DateTime|int $delay
	 *
	 * @return string
	 */
	private function declareFailedQueue($destination, $delay)
	{

		$delay = $this->getSeconds($delay);
		$destination = $this->getQueueName($destination);
		$name = $this->getQueueName($destination) . '_failed_' . $delay;
		$exchange = $this->configExchange['name'] ?:$name;

		// declare exchange
		$this->channel->exchange_declare(
			$exchange,
			$this->configExchange['type'],
			$this->configExchange['passive'],
			$this->configExchange['durable'],
			$this->configExchange['auto_delete']
		);

		// declare queue
		$this->channel->queue_declare(
			$name,
			$this->configQueue['passive'],
			$this->configQueue['durable'],
			$this->configQueue['exclusive'],
			$this->configQueue['auto_delete'],
			false,
			new AMQPTable([
				'x-dead-letter-exchange'    => $destination,
				'x-dead-letter-routing-key' => $destination,
				'x-message-ttl'             => $delay * 1000,
			])
		);

		// bind queue to the exchange
		$this->channel->queue_bind($name, $exchange, $name);

		return $name;
	}

	public function getJobsOnQueue($queue = null)
	{
		$queue = $this->getQueueName($queue);

		// declare queue if not exists
		$this->declareQueue($queue);

		// get envelope
		$message = $this->channel->basic_get($queue);

		if ($message instanceof AMQPMessage) {
			return new RabbitMQJob($this->container, $this, $this->channel, $queue, $message);
		}

		return null;		
	}

	public function getAllFailedJobs()
	{
		$returnArray = array();
		$client = APIClient::factory(array('host' => env('RABBITMQ_QUEUE_QUERY_HOST')));
		$queues = $client->listQueues();

		foreach ($queues as $k => $v) {
			if (preg_match('/_failed_/', $v->name))
			{
				$url = 'http://' . env('RABBITMQ_QUEUE_QUERY_HOST') . ':'. env('RABBITMQ_QUEUE_QUERY_HOST_PORT') .'/api/queues/%2f/' . $v->name . '/get';
				$client = new Client();
				$body = ['count' => $v->messages, 'requeue' => true, 'encoding' => 'auto'];
				$request = $client->post($url, ['content-type' => 'application/json', 'auth' => ['guest', 'guest'], 'body' => \GuzzleHttp\json_encode($body)]);
				$messagesArray = \GuzzleHttp\json_decode($request->getBody()->getContents());
				for($i=0; $i<sizeof($messagesArray); $i++) {
					$a = new \stdClass();
					$a->id = $messagesArray[$i]->message_count;
					$a->connection = env('QUEUE_DRIVER');
					$a->queue = $v->name;
					$a->payload =  $messagesArray[$i]->payload;
					//$a->failed_at = date('Y-m-d H:i:s');
					$returnArray[] = $a;
				}
			}
		}
		return $returnArray;

	}

}
