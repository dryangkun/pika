### 新增命令说明
##### bnhmin/bnhmax(key, field, int)
```
作用类似hset，

bnhmin来说，原有值如果大于新值，则写入新值，并且返回0，否则返回1

bnhmax来说，原有值如果小于新值，则写入新值，并且返回0，否则返回1
```

##### bnmsetex(key1, val1, key2, val2..., ttl)
```
相比较于mset，就是多了可以设置ttl
```

##### bnstream(key, val1, val2...)
```
类似kafka方式，允许消息可以重复消费，举例说明：

bnstream("a", "1", "2", "3")，则最终生成的kv数据如下：

key . chr(255) . offset
"a" . chr(255) . "0000000015d1ae26a0000000000000000" => "1"
"a" . chr(255) . "0000000015d1ae26a0000000000000001" => "2"
"a" . chr(255) . "0000000015d1ae26a0000000000000002" => "3"
offset保证是递增（字典序）
```

##### bnhscriptload scriptname scriptcontent
```
存储lua脚本内容
bnhscriptload test_script 'pika_hset("z", 123) return nil, "wrong xxx"'
```

##### bnheval scriptname hashkey args
```
调用lua脚本操作hash
bnheval test_script k 1 2 3 4
```

##### 例：bnhistoryrange的lua实现
```
bnhscriptload bnhistoryrange "
local old_id = ARGS[1]
local history_field = ARGS[2]
local value = tonumber(ARGS[3])
local range = tonumber(ARGS[4])
local ival = pika_hget(history_field)
local tmp_flag = false
local code = 0
local iival = tonumber(ival)
if(ival) then 
	if(value > iival) then
		pika_hset(history_field, tostring(value))
        	if (value - iival > range) then
                	tmp_flag = true
        	end
	end
else
	pika_hset(history_field, tostring(value))
	tmp_flag = true
end

local oival = pika_hget(old_id)
if (not oival) then 
	pika_hset(old_id,tostring(value))
	if (tmp_flag) then 
		code = 1
	end
end
return code"
```
###### 使用
```
bnheval bnhistoryrange u_1 y history 1 15
```