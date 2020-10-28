### CEP简介

----

#### 什么是CEP

- 复杂事件处理
- Flink CEP是在Flink中实现的复杂事件处理库
- CEP允许在无休无止的事件流中检测事件模式，让我们有机会掌握数据中重要的部分
- 一个或多个由简单事件构成的事件流通过一定的规则匹配，然后输出用户想得到的数据，即满足规则的复杂事件

### CEP的特点

- 目标: 从有序的简单事件流中发现一些高阶特征
- 输入: 一个或多个由简单事件构成的事件流
- 处理: 识别简单事件之间的内在联系，多个符合一定规则的简单事件构成复杂事件
- 输出: 满足规则的复杂事件

### Pattern API

- 处理事件的规则，叫模式Pattern
- Flink CEP提供Pattern API用于对输入流数据进行复杂事件规则定义，用来提取符合规则的事件序列

```scala
val pattern = Pattern.begin[Event]("start").where(_.getId == 42)
    .next("middle").subtype(classOf[SubEvent]).where(_.getTemp >= 10.0)
    .followedBy("end").where(_.getName == "end")

val patternStream = CEP.pattern(inputDataStream, pattern)
val result: DataStream[Alert] = patternStream.select(createAlert(_))
```

#### 个体模式

- 组成复杂规则的每一个单独的模式定义就是个体模式

```scala
start.times(3).where(_.behavior.startWith("fav"))
```

单例模式: 只接收一个事件

循环模式: 可以接收多个事件

- 量词

```scala
// 匹配出现四次
start.times(4)
// 匹配出现2、3或者4次，并且尽可能多的重复匹配
start.times(2, 4).greedy
// 匹配出现0或4次
start.times(4).optional
// 匹配出现一次或多次
start.oneOrMore
// 匹配出现2、3或4次
start.times(2, 4)
// 匹配出现0、2或多次并且尽可能多重复匹配
start.timesOrMore(2).optional.greedy
```

- 条件
  - 每个模式需要指定触发条件，作为模式是否接受事件进入的判断依据
  - 主要通过.where、.or、.until指定条件
  - 按调用方式区分，可分为以下几类
    - 简单条件(where)
    - 组合条件 .or .where
    - 终止条件 .until 如果使用了oneOrMore、oneOrMore.optional建议加上 .until作为终止条件
    - 迭代条件: 对模式之前所有接收的事件进行处理 .where((condition, ctx) => {}) ctx.getEventsForPattern("")

#### 组合模式(序列模式)

- 很多个个体模式组合起来，就形成了整个模式序列

- 模式序列必须以一个初始模式开始

  

   近邻模式

- 严格近邻模式

  .next

- 宽松近邻模式

  followedBy

- 非确定性宽松近邻

  .followedByAny

- notNext
- notFollowedBy

注意

- 所有模式都以.begin()开始
- 不能以.notFollowedBy()结束
- not类型的模式不能被optional修饰
- 可以为模式指定时间约束，用来要求在多长时间内匹配有效 .within(Time)

#### 模式组

- 将一个模式序列作为条件嵌套在个体模式里，成为一组模式

### 模式的检测

CEP.pattern(input, pattern)

匹配事件的提取

- select
- flatSelect

### 超时事件的提取

```scala
val patternStream = CEP.pattern(input, pattern)
val outputTag = new OutputTag[String]("side-output")
val result = patternStream.select(outputTag) {
  (pattern: Map[String, Iterable[Event]], timestamp: Long) => TimeoutEvent()
} {
  (pattern: Map[String, Iterable[Event]], timestamp: Long) => ComplexEvent()
}

val timeoutResult = result.getSideOutput(outputTag)
```

