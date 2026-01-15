## 版本

是基于[pikiwidb](https://github.com/OpenAtomFoundation/pikiwidb/tree/v3.0.16)开发而来，增加以下的命令

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
类似kafka方式，允许消息可以重复消费，数据保存7天，举例说明：

bnstream("a", "1", "2", "3")，则最终生成的kv数据如下：

key . chr(255) . offset
"a" . chr(255) . "0000000015d1ae26a0000000000000000" => "1"
"a" . chr(255) . "0000000015d1ae26a0000000000000001" => "2"
"a" . chr(255) . "0000000015d1ae26a0000000000000002" => "3"
offset保证是递增（字典序）

读取使用命令 = pkscanrange string/string_with_value 'a'.chr(255).'0' '' limit 10
```

##### hash结构的lua支持
##### 依赖与luajit-5.1，需先安装luajit-5.1
###### bnhscriptload scriptname scriptcontent
```
作用：初始化lua脚本
示例：bnhscriptload test_script 'local x1 = ARGS[1]; pika_hset("z", tostring(x1)); local x = pika_hget("z"); pika_hset("z1", tostring(x)); return nil, "wrong xxx"'
注意一：整个lua脚本的执行期间会对hash结构上排他锁，命令执行完成后解锁
注意二：pika_hset可以设置hash的field对应的值，调用pika_hget可以获取hash的field对应的值，输入的数据从ARGS数组读取
注意三：pika_hset的执行是批量的，因此对某个filed先执行pika_hset，在pika_hget，会发现值并不是刚pika_hset的值，而是执行命令前的值-因为数据写入是在命令执行的最后
注意四：return 一般情况下直接return想要返回的值即可，如果想要返回错误，参考例子return nil, "错误信息"
```

###### bnheval scriptname hashkey args
```
作用：调用lua脚本操作hash
示例：bnheval test_script k 1 2 3 4
注意一：k为hash的key，后面跟随的是lua的参数，使用ARGS数组接收即可
```