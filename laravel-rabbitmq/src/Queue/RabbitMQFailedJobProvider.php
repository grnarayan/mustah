<?php

namespace Mustah\Queue;


use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Mustah\Queue\Connectors\RabbitMQConnector;

class RabbitMQFailedJobProvider implements FailedJobProviderInterface
{
    protected $rabbitMQConnector;
    protected $rabbitMQQueue;

    /**
     * Create a new rabbitmq failed job provider.
     *
     * @param  array $config
     */
    public function __construct($config)
    {
        $this->rabbitMQConnector = new RabbitMQConnector();
        $this->rabbitMQQueue = $this->rabbitMQConnector->connect($config);
    }

    /**
     * Log a failed job into storage.
     *
     * @param  string  $connection
     * @param  string  $queue
     * @param  string  $payload
     * @return void
     */
    public function log($connection, $queue, $payload)
    {
        
        $options = array('failed' => env('RABBITMQ_FAILED_TTL'));
        $this->rabbitMQQueue->pushRaw($payload, $queue, $options);
    }

    /**
     * Get a list of all of the failed jobs.
     *
     * @return array
     */
    public function all()
    {
        return $this->rabbitMQQueue->getAllFailedJobs();
    }

    /**
     * Get a single failed job.
     *
     * @param  mixed  $id
     * @return array
     */
    public function find($id)
    {
        //return $this->getTable()->find($id);
        return 'find - Need to implement this method';
    }

    /**
     * Delete a single failed job from storage.
     *
     * @param  mixed  $id
     * @return bool
     */
    public function forget($id)
    {
        //return $this->getTable()->where('id', $id)->delete() > 0;
        return 'forget - Need to implement this method';

    }

    /**
     * Flush all of the failed jobs from storage.
     *
     * @return void
     */
    public function flush()
    {
        return $this->rabbitMQQueue->flushJobs();
    }

    /**
     * Get a new query builder instance for the table.
     *
     * @return \Illuminate\Database\Query\Builder
     */
    protected function getTable()
    {
        //return $this->resolver->connection($this->database)->table($this->table);
        return 'getTable - Need to implement this method';
    }
}
