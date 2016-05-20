<?php
namespace Mustah;

use Illuminate\Queue\Failed\DatabaseFailedJobProvider;
use Illuminate\Queue\Failed\NullFailedJobProvider;
use Illuminate\Support\ServiceProvider;
use Mustah\Queue\Connectors\RabbitMQConnector;
use Mustah\Queue\RabbitMQFailedJobProvider;

class RabbitMQServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        $this->mergeConfigFrom(
            __DIR__.'/config/rabbitmq.php', 'queue.connections.rabbitmq'
        );
    }

    /**
     * Register the application's event listeners.
     *
     * @return void
     */
    public function boot()
    {
        /**
         * @var \Illuminate\Queue\QueueManager $manager
         */
        $manager = $this->app['queue'];
        $manager->addConnector('rabbitmq', function () {
            return new RabbitMQConnector();
        });
        $this->registerFailedJobServices();
    }

    protected function registerFailedJobServices()
    {
        $this->app->singleton('queue.failer', function ($app) {
            $config = $app['config']['queue.failed'];

            switch (env('FAILED_QUEUE_DRIVER')) {
                case 'rabbitmq':
                    if (isset($config['rabbitmq']))
                        return new RabbitMQFailedJobProvider($config['rabbitmq']);
                    else
                        return new NullFailedJobProvider();
                    break;
                case 'database':
                    if (isset($config['table']))
                        return new DatabaseFailedJobProvider($app['db'], $config['database'], $config['table']);
                    else
                        return new NullFailedJobProvider();
                    break;
                default:
                    return new NullFailedJobProvider();
                    break;
            }
//            if (isset($config['rabbitmq'])) {
//                return new RabbitMQFailedJobProvider($config['rabbitmq']);
//            } elseif (isset($config['table'])) {
//                return new DatabaseFailedJobProvider($app['db'], $config['database'], $config['table']);
//            }
//            else {
//                return new NullFailedJobProvider();
//            }

        });
    }
}
