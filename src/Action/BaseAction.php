<?php

namespace Action;

use Com\Alibaba\Otter\Canal\Protocol\Column;
use Chenlin\CanalPhp\DealInterface\DealInterface;


abstract class BaseAction
{

    /** @var array $entrys [包含的都是Entry] */
    private  $entrys = [];

    /**
     * 构造函数
     * DealProvinceAction constructor.
     * @param array  $entry [数据类型为: Entry]
     */
    public function __construct(array $entry)
    {
        $this->entrys = $entry;
    }



    /** 自定义同步数据的业务逻辑 */
    abstract public function async(DealInterface $deal);

    /**
     * 转化记录数据格式
     * @param Column $columns
     * @return array
     */
    public function dealColumn($columns): array
    {

        $field_list = [];
        /** @var Column $column */
        foreach ($columns as $column) {
            $field_list[$column->getName()] = ['value' => $column->getValue(), 'update' => $column->getUpdated()];
        }
        return $field_list;
    }
}