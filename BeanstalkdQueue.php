<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\queue;

use Pheanstalk\Pheanstalk;
use yii\base\Component;
use yii\base\InvalidConfigException;
use yii\helpers\Json;

/**
 * BeanstalkdQueue
 *
 * @author Anton Neznaenko <neanton@gmail.com>
 */
class BeanstalkdQueue extends Component implements QueueInterface
{
    /**
     * @var Pheanstalk|array
     */
    public $pheanstalk;

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();

        if (null === $this->pheanstalk) {
            throw new InvalidConfigException('The "pheanstalk" property must be set.');
        }

        if (!$this->pheanstalk instanceof Pheanstalk) {
            $this->pheanstalk = new Pheanstalk(
                $this->pheanstalk['host'],
                $this->pheanstalk['port'],
                $this->pheanstalk['connectTimeout'],
                $this->pheanstalk['connectPersistent']
            );
        }
    }

    /**
     * @inheritdoc
     */
    public function push($payload, $queue, $delay = 0)
    {
        $data = Json::encode($payload);
        return $this->pheanstalk->putInTube($queue, $data, Pheanstalk::DEFAULT_PRIORITY, $delay);
    }

    /**
     * @inheritdoc
     */
    public function pop($queue)
    {
        $job = $this->pheanstalk->reserveFromTube($queue);

        return [
            'id' => $job->getId(),
            'body' => Json::decode($job->getData()),
            'queue' => $queue,
        ];
    }

    /**
     * @inheritdoc
     */
    public function purge($queue)
    {
        while ($job = $this->pheanstalk->reserveFromTube($queue)) {
            $this->pheanstalk->delete($job);
        }
    }

    /**
     * @inheritdoc
     */
    public function release(array $message, $delay = 0)
    {
        $job = $this->pheanstalk->peek($message['id']);
        $this->pheanstalk->release($job, Pheanstalk::DEFAULT_PRIORITY, $delay);
    }

    /**
     * @inheritdoc
     */
    public function delete(array $message)
    {
        $job = $this->pheanstalk->peek($message['id']);
        $this->pheanstalk->delete($job);
    }
}
