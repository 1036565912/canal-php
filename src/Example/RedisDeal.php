<?php

namespace Example;

use Chenlin\CanalPhp\DealInterface\DealInterface;

class RedisDeal implements DealInterface
{

    //由于数据来源为mysql  则这里需要设置主键
    protected  $pk = 'id';

    //检测的字段 [需要检测的是否发生变化的字段]
    protected  $check_field = ['name'];

    //redis中key的值
    protected  $redis_key = 'id';

    protected  $redis = null;

    public function __construct(array $redis_config)
    {
        $this->redis = new \Redis();
        if (!$this->redis->connect($redis_config['host'], $redis_config['port'], $redis_config['timeout'])) {
            throw new \Exception('redis connect fail!');
        }

        $this->select($redis_config['db_index']);
    }

    /**
     * @param $name
     * @param $arguments
     * @return mixed
     */
    public function __call($name, $arguments)
    {
        // TODO: Implement __call() method.
        return call_user_func_array([$this->redis,$name], $arguments);
    }

    /**
     * 检测到数据库存在更新的数据[这是一条完整的记录]
     * @param array $record
     * @return bool
     */
    public function update(array $record): bool
    {
        // TODO: Implement update() method.
        $res = [];
        foreach ($record as $field => $val) {
            if (in_array($field,$this->check_field)) {
                if ($val['update']) {
                    $res[$field] = $val['value'];
                }
            }
        }

        //开始同步数据
        if (count($this->check_field) == 1) {
            $res = $res[$this->check_field[0]];
        } else {
            $res = json_encode($res);
        }
        return $this->set($record[$this->redis_key]['value'], $res);
    }

    public function insert(array $record): bool
    {
        // TODO: Implement insert() method.
        $res = [];
        foreach ($record as $field => $val) {
            if (in_array($field,$this->check_field)) {
                if ($val['update']) {
                    $res[$field] = $val['value'];
                }
            }
        }

        //开始同步数据
        if (count($this->check_field) == 1) {
            $res = $res[$this->check_field[0]];
        } else {
            $res = json_encode($res);
        }
        return $this->set($record[$this->redis_key]['value'], $res);
    }



    public function delete(array $record): bool
    {
        // TODO: Implement delete() method.
        return $this->del($record[$this->redis_key]['value']) ? true : false;
    }
}