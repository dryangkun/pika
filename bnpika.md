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
##### 依赖与luajit-5.1，需先安装luajit-5.1
```
存储lua脚本内容
bnhscriptload test_script 'local x1 = ARGS[1]; pika_hset("z", tostring(x1)); local x = pika_hget("z"); pika_hset("z1", tostring(x)); return nil, "wrong xxx"'
调用pika_hset可以设置hash的field对应的值，调用pika_hget可以获取hash的field对应的值，输入的数据从ARGS数组读取
注意的是pika_hset的执行是批量的，因此对某个filed先执行pika_hset，在pika_hget，会发现值并不是刚pika_hset的值，而是执行命令前的值-因为数据写入是在命令执行的最后
```

##### bnheval scriptname hashkey args
```
调用lua脚本操作hash
bnheval test_script k 1 2 3 4
k为hash的key，后面跟随的是lua的参数，使用ARGS数组接收即可
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